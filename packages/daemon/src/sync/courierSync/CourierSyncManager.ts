import { v4 as getDefaultGateway } from 'default-gateway';
import pipe from 'it-pipe';
import { waitUntilUsedOnHost } from 'tcp-port-used';
import { Inject, Service } from 'typedi';

import { COURIER_PORT, CourierConnectionStatus, CourierSyncStage } from '.';
import { Config } from '../../Config';
import { UnregisteredGatewayError } from '../../errors';
import { fork } from '../../utils/subprocess/child';
import { IPCMessage } from '../ipc';
import { GatewayRegistrar } from '../publicGateway/GatewayRegistrar';
import { DisconnectedFromCourierError } from './errors';
import { CourierSyncStageNotification } from './messaging';

const COURIER_CHECK_TIMEOUT_MS = 3_000;
const COURIER_CHECK_RETRY_MS = 500;

@Service()
export class CourierSyncManager {
  constructor(
    @Inject() protected gatewayRegistrar: GatewayRegistrar,
    @Inject() protected config: Config,
  ) {}

  public async *streamStatus(): AsyncIterable<CourierConnectionStatus> {
    let lastStatus: CourierConnectionStatus | null = null;
    while (true) {
      const newStatus = await this.getCourierConnectionStatus();
      if (newStatus !== lastStatus) {
        lastStatus = newStatus;
        yield newStatus;
      }
    }
  }

  /**
   * Synchronise with a courier.
   *
   * @throws UnregisteredGatewayError
   * @throws DisconnectedFromCourierError
   */
  public async *sync(): AsyncIterable<CourierSyncStage> {
    const syncSubprocess = fork('courier-sync');
    yield* await pipe(
      wrapSubprocessErrors(syncSubprocess),
      async function* (messages: AsyncIterable<IPCMessage>): AsyncIterable<CourierSyncStage> {
        for await (const message of messages) {
          if (message.type !== 'stage') {
            continue;
          }
          const messageTyped = message as CourierSyncStageNotification;
          if (messageTyped.stage in CourierSyncStage) {
            yield messageTyped.stage;
          }
        }
      },
    );
  }

  /**
   * Get the system's default gateway IPv4 address, if connected to a network.
   *
   * @throws DisconnectedFromCourierError if the default gateway couldn't be found (e.g., the
   *    device isn't connected to any network)
   */
  protected async getDefaultGatewayIPAddress(): Promise<string> {
    try {
      const { gateway: defaultGatewayIPAddress } = await getDefaultGateway();
      return defaultGatewayIPAddress;
    } catch (err) {
      throw new DisconnectedFromCourierError(err, 'Could not find default system gateway');
    }
  }

  protected async getCourierConnectionStatus(): Promise<CourierConnectionStatus> {
    let gatewayIPAddress: string;
    try {
      gatewayIPAddress = await this.getDefaultGatewayIPAddress();
    } catch (_) {
      return CourierConnectionStatus.DISCONNECTED;
    }
    try {
      await waitUntilUsedOnHost(
        COURIER_PORT,
        gatewayIPAddress,
        COURIER_CHECK_RETRY_MS,
        COURIER_CHECK_TIMEOUT_MS,
      );
    } catch (_) {
      return CourierConnectionStatus.DISCONNECTED;
    }
    return CourierConnectionStatus.CONNECTED;
  }
}

async function* wrapSubprocessErrors(
  messages: AsyncIterable<IPCMessage>,
): AsyncIterable<IPCMessage> {
  try {
    yield* await messages;
  } catch (err) {
    throw err?.exitCode === 1
      ? new UnregisteredGatewayError('Private gateway is unregistered')
      : new DisconnectedFromCourierError(err, 'Courier sync failed');
  }
}
