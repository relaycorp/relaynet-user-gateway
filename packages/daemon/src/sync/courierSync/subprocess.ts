import { CogRPCClient } from '@relaycorp/cogrpc';
import {
  Cargo,
  CargoCollectionAuthorization,
  CargoCollectionRequest,
  CargoDeliveryRequest,
  CargoMessageSet,
  CargoMessageStream,
  Certificate,
  Gateway,
  issueGatewayCertificate,
  Parcel,
  ParcelCollectionAck,
  RecipientAddressType,
  SessionlessEnvelopedData,
  UnboundKeyPair,
} from '@relaycorp/relaynet-core';
import bufferToArray from 'buffer-to-arraybuffer';
import { addDays, differenceInSeconds, subMinutes } from 'date-fns';
import { v4 as getDefaultGateway } from 'default-gateway';
import pipe from 'it-pipe';
import { Logger } from 'pino';
import { Duplex } from 'stream';
import { Container } from 'typedi';
import { getRepository } from 'typeorm';
import uuid from 'uuid-random';

import { COURIER_PORT, CourierSyncExitCode, CourierSyncStage } from '.';
import { ParcelCollection } from '../../entity/ParcelCollection';
import { DBPrivateKeyStore } from '../../keystores/DBPrivateKeyStore';
import { DBPublicKeyStore } from '../../keystores/DBPublicKeyStore';
import { ParcelStore } from '../../parcelStore';
import { LOGGER } from '../../tokens';
import { MessageDirection } from '../../utils/MessageDirection';
import { sleepSeconds } from '../../utils/timing';
import { GatewayRegistrar } from '../publicGateway/GatewayRegistrar';
import { PublicGateway } from '../publicGateway/PublicGateway';
import { CourierSyncStageNotification } from './messaging';

const DELAY_BETWEEN_COLLECTION_AND_DELIVERY_SECONDS = 5;

const CLOCK_DRIFT_TOLERANCE_MINUTES = 90;
const OUTBOUND_CARGO_TTL_DAYS = 14;

export default async function runCourierSync(parentStream: Duplex): Promise<number> {
  const gatewayRegistrar = Container.get(GatewayRegistrar);
  const logger = Container.get(LOGGER);

  const publicGateway = await gatewayRegistrar.getPublicGateway();
  if (!publicGateway) {
    logger.fatal('Private gateway is unregistered');
    return CourierSyncExitCode.UNREGISTERED_GATEWAY;
  }

  let defaultGatewayIPAddress: string;
  try {
    const defaultGateway = await getDefaultGateway();
    defaultGatewayIPAddress = defaultGateway.gateway;
  } catch (err) {
    logger.fatal({ err }, 'System default gateway could not be found');
    return CourierSyncExitCode.FAILED_SYNC;
  }

  const parcelStore = Container.get(ParcelStore);
  const privateKeyStore = Container.get(DBPrivateKeyStore);
  const publicKeyStore = Container.get(DBPublicKeyStore);
  const gateway = new Gateway(privateKeyStore, publicKeyStore);
  const currentKey = (await privateKeyStore.getCurrentKey())!;
  let client: CogRPCClient | null = null;
  try {
    client = await CogRPCClient.init(`https://${defaultGatewayIPAddress}:${COURIER_PORT}`);

    await collectCargo(
      publicGateway,
      gateway,
      client,
      currentKey,
      parcelStore,
      parentStream,
      privateKeyStore,
      logger,
    );

    sendStageNotificationToParent(parentStream, CourierSyncStage.WAIT);
    await sleepSeconds(DELAY_BETWEEN_COLLECTION_AND_DELIVERY_SECONDS);

    await deliverCargo(
      publicGateway,
      gateway,
      client,
      currentKey,
      parcelStore,
      parentStream,
      logger,
    );
  } catch (err) {
    logger.fatal({ err }, 'Sync failed');
    return CourierSyncExitCode.FAILED_SYNC;
  } finally {
    client?.close();
  }

  logger.info('Sync completed successfully');
  return CourierSyncExitCode.OK;
}

async function collectCargo(
  publicGateway: PublicGateway,
  privateGateway: Gateway,
  client: CogRPCClient,
  currentKey: UnboundKeyPair,
  parcelStore: ParcelStore,
  parentStream: Duplex,
  privateKeyStore: DBPrivateKeyStore,
  logger: Logger,
): Promise<void> {
  sendStageNotificationToParent(parentStream, CourierSyncStage.COLLECTION);

  const ccaSerialized = await generateCCA(publicGateway, currentKey, privateKeyStore);
  await pipe(
    client.collectCargo(ccaSerialized),
    async (cargoesSerialized: AsyncIterable<Buffer>) => {
      // We shouldn't *have* to filter self-issued certificates, but PKI.js would hang if we don't
      // filter out trusted certificates with a Subject Key Identifier (SKI) that matches that of
      // the end entity certificate.
      const ownCertificates = await privateKeyStore.fetchNodeCertificates();
      const ownCDACertificates = await filterSelfIssuedCertificates(ownCertificates);

      for await (const cargoSerialized of cargoesSerialized) {
        let cargo: Cargo;
        try {
          cargo = await Cargo.deserialize(bufferToArray(cargoSerialized));
        } catch (err) {
          logger.info({ err }, 'Ignoring malformed/invalid cargo');
          continue;
        }
        const cargoAwareLogger = logger.child({ cargoId: cargo.id });

        try {
          await cargo.validate(RecipientAddressType.PRIVATE, ownCDACertificates);
        } catch (err) {
          cargoAwareLogger.info({ err }, 'Ignoring cargo by unauthorized sender');
          continue;
        }

        let cargoMessageSet: CargoMessageSet;
        try {
          cargoMessageSet = await privateGateway.unwrapMessagePayload(cargo);
        } catch (err) {
          cargoAwareLogger.info({ err }, 'Ignored cargo with invalid payload');
          continue;
        }

        for (const itemSerialized of cargoMessageSet.messages) {
          let item: Parcel | ParcelCollectionAck;
          try {
            item = await CargoMessageSet.deserializeItem(itemSerialized);
          } catch (err) {
            cargoAwareLogger.info({ err }, 'Ignoring invalid/malformed message');
            continue;
          }
          if (item instanceof Parcel) {
            await processParcel(
              item,
              itemSerialized,
              parcelStore,
              ownCertificates,
              cargoAwareLogger,
            );
          } else {
            await processParcelCollectionAck(item, parcelStore, cargoAwareLogger);
          }
        }
      }
    },
  );
}

async function generateCCA(
  publicGateway: PublicGateway,
  currentKey: UnboundKeyPair,
  privateKeyStore: DBPrivateKeyStore,
): Promise<Buffer> {
  const now = new Date();
  const startDate = subMinutes(now, CLOCK_DRIFT_TOLERANCE_MINUTES);
  const endDate = addDays(now, OUTBOUND_CARGO_TTL_DAYS);

  const recipientAddress = `https://${publicGateway.publicAddress}`;
  const ccaIssuer = await privateKeyStore.getOrCreateCCAIssuer();
  const cargoDeliveryAuthorization = await issueGatewayCertificate({
    issuerCertificate: ccaIssuer,
    issuerPrivateKey: currentKey.privateKey,
    subjectPublicKey: await publicGateway.identityCertificate.getPublicKey(),
    validityEndDate: endDate,
  });
  const ccr = new CargoCollectionRequest(cargoDeliveryAuthorization);
  const ccaPayload = await SessionlessEnvelopedData.encrypt(
    await ccr.serialize(),
    publicGateway.identityCertificate,
  );
  const cca = new CargoCollectionAuthorization(
    recipientAddress,
    currentKey.certificate,
    Buffer.from(ccaPayload.serialize()),
    { creationDate: startDate, ttl: differenceInSeconds(endDate, startDate) },
  );
  const ccaSerialized = await cca.serialize(currentKey.privateKey);
  return Buffer.from(ccaSerialized);
}

async function processParcel(
  parcel: Parcel,
  parcelSerialized: ArrayBuffer,
  parcelStore: ParcelStore,
  ownCertificates: readonly Certificate[],
  logger: Logger,
): Promise<void> {
  try {
    await parcel.validate(RecipientAddressType.PRIVATE, ownCertificates);
  } catch (err) {
    logger.info({ err }, 'Ignoring invalid parcel');
    return;
  }
  const parcelKey = await parcelStore.storeEndpointBound(Buffer.from(parcelSerialized), parcel);
  logger.info(
    { parcel: { id: parcel.id, key: parcelKey, recipientAddress: parcel.recipientAddress } },
    'Stored parcel',
  );
}

async function processParcelCollectionAck(
  ack: ParcelCollectionAck,
  parcelStore: ParcelStore,
  logger: Logger,
): Promise<void> {
  logger.info(
    {
      parcel: {
        id: ack.parcelId,
        recipientAddress: ack.recipientEndpointAddress,
        senderAddress: ack.senderEndpointPrivateAddress,
      },
    },
    'Deleting ACKed parcel',
  );
  await parcelStore.deleteInternetBoundFromACK(ack);
}

async function deliverCargo(
  publicGateway: PublicGateway,
  privateGateway: Gateway,
  client: CogRPCClient,
  currentKey: UnboundKeyPair,
  parcelStore: ParcelStore,
  parentStream: Duplex,
  logger: Logger,
): Promise<void> {
  sendStageNotificationToParent(parentStream, CourierSyncStage.DELIVERY);

  const cargoDeliveryStream = makeCargoDeliveryStream(
    publicGateway,
    privateGateway,
    currentKey,
    parcelStore,
    logger,
  );
  await pipe(client.deliverCargo(cargoDeliveryStream), async (ackIds: AsyncIterable<string>) => {
    for await (const ackId of ackIds) {
      logger.debug({ ackId }, 'Received parcel delivery acknowledgement');
    }
  });
}

async function* makeCargoDeliveryStream(
  publicGateway: PublicGateway,
  privateGateway: Gateway,
  currentKey: UnboundKeyPair,
  parcelStore: ParcelStore,
  logger: Logger,
): AsyncIterable<CargoDeliveryRequest> {
  const cargoStream = privateGateway.generateCargoes(
    makeCargoMessageStream(parcelStore, logger),
    publicGateway.identityCertificate,
    currentKey.privateKey,
    currentKey.certificate,
    `https://${publicGateway.publicAddress}`,
  );
  yield* await pipe(
    cargoStream,
    async function* (cargoes: AsyncIterable<Buffer>): AsyncIterable<CargoDeliveryRequest> {
      for await (const cargo of cargoes) {
        yield { cargo, localId: uuid() };
      }
    },
  );
}

async function* makeCargoMessageStream(
  parcelStore: ParcelStore,
  logger: Logger,
): CargoMessageStream {
  const collectionRepo = getRepository(ParcelCollection);
  const pendingACKs = await collectionRepo.find();
  for (const pendingACK of pendingACKs) {
    const ack = new ParcelCollectionAck(
      pendingACK.senderEndpointPrivateAddress,
      pendingACK.recipientEndpointAddress,
      pendingACK.parcelId,
    );
    yield { message: Buffer.from(ack.serialize()), expiryDate: pendingACK.parcelExpiryDate };
  }

  for await (const parcelWithExpiryDate of parcelStore.listInternetBound()) {
    const parcelSerialized = await parcelStore.retrieve(
      parcelWithExpiryDate.parcelKey,
      MessageDirection.TOWARDS_INTERNET,
    );
    if (parcelSerialized) {
      yield { message: parcelSerialized, expiryDate: parcelWithExpiryDate.expiryDate };
    } else {
      logger.debug({ parcelKey: parcelWithExpiryDate.parcelKey }, 'Skipped deleted parcel');
    }
  }
}

function sendStageNotificationToParent(parentStream: Duplex, stage: CourierSyncStage): void {
  const stageNotification: CourierSyncStageNotification = {
    stage,
    type: 'stage',
  };
  parentStream.write(stageNotification);
}

async function filterSelfIssuedCertificates(
  certificates: readonly Certificate[],
): Promise<readonly Certificate[]> {
  // tslint:disable-next-line:readonly-array
  const selfIssuedCertificates = [];
  for (const certificate of certificates) {
    if (
      certificate.getIssuerPrivateAddress() === (await certificate.calculateSubjectPrivateAddress())
    ) {
      selfIssuedCertificates.push(certificate);
    }
  }
  return selfIssuedCertificates;
}
