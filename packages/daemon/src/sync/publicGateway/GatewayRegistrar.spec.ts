import {
  Certificate,
  derSerializePublicKey,
  GSCClient,
  PrivateNodeRegistration,
  PrivateNodeRegistrationRequest,
} from '@relaycorp/relaynet-core';
import { generateNodeKeyPairSet, generatePDACertificationPath } from '@relaycorp/relaynet-testing';
import { Container } from 'typedi';

import { Config } from '../../Config';
import { DEFAULT_PUBLIC_GATEWAY } from '../../constants';
import { arrayBufferFrom } from '../../testUtils/buffer';
import { makeConfigTokenEphemeral } from '../../testUtils/config';
import { setUpTestDBConnection } from '../../testUtils/db';
import { PreRegisterNodeCall, RegisterNodeCall } from '../../testUtils/gscClient/methodCalls';
import { MockGSCClient } from '../../testUtils/gscClient/MockGSCClient';
import { mockSpy } from '../../testUtils/jest';
import { mockPrivateKeyStore } from '../../testUtils/keystores';
import { PUBLIC_GATEWAY_ADDRESS } from '../../tokens';
import { GatewayRegistrar } from './GatewayRegistrar';
import * as gscClient from './gscClient';

setUpTestDBConnection();

const privateKeyStore = mockPrivateKeyStore();

let registrar: GatewayRegistrar;
beforeEach(() => {
  registrar = Container.get(GatewayRegistrar);
});

let mockGSCClient: GSCClient | null;
beforeEach(() => {
  mockGSCClient = null;
});

const mockMakeGSCClient = mockSpy(jest.spyOn(gscClient, 'makeGSCClient'), () => mockGSCClient);

makeConfigTokenEphemeral(PUBLIC_GATEWAY_ADDRESS);

describe('register', () => {
  let publicGatewayIdCertificate: Certificate;
  let idCertificate: Certificate;
  beforeAll(async () => {
    const certPath = await generatePDACertificationPath(await generateNodeKeyPairSet());
    publicGatewayIdCertificate = certPath.publicGateway;
    idCertificate = certPath.privateGateway;
  });

  const registrationAuth = arrayBufferFrom('the auth');
  let preRegisterCall: PreRegisterNodeCall;
  let registration: PrivateNodeRegistration;
  let registerCall: RegisterNodeCall;
  beforeEach(() => {
    preRegisterCall = new PreRegisterNodeCall(registrationAuth);

    registration = new PrivateNodeRegistration(idCertificate, publicGatewayIdCertificate);
    registerCall = new RegisterNodeCall(registration);

    mockGSCClient = new MockGSCClient([preRegisterCall, registerCall]);
  });

  test('Registration should be skipped if already registered with new gateway', async () => {
    const config = Container.get(Config);
    await config.set(PUBLIC_GATEWAY_ADDRESS, DEFAULT_PUBLIC_GATEWAY);

    await registrar.register(DEFAULT_PUBLIC_GATEWAY);

    expect(mockMakeGSCClient).not.toBeCalled();
    expect(preRegisterCall.wasCalled).toBeFalsy();
  });

  test('PoWeb client should complete registration with resolved endpoint', async () => {
    await registrar.register(DEFAULT_PUBLIC_GATEWAY);

    expect(mockMakeGSCClient).toBeCalledWith(DEFAULT_PUBLIC_GATEWAY);
  });

  test('PoWeb client should do pre-registration', async () => {
    await registrar.register(DEFAULT_PUBLIC_GATEWAY);

    expect(preRegisterCall.wasCalled).toBeTruthy();
  });

  test('PoWeb client should complete registration with given authorisation', async () => {
    await registrar.register(DEFAULT_PUBLIC_GATEWAY);

    expect(registerCall.wasCalled).toBeTruthy();
    const registrationRequest = await PrivateNodeRegistrationRequest.deserialize(
      registerCall.arguments!.pnrrSerialized,
    );
    await expect(derSerializePublicKey(registrationRequest.privateNodePublicKey)).resolves.toEqual(
      await derSerializePublicKey(preRegisterCall.arguments!.nodePublicKey),
    );
    expect(registrationRequest.pnraSerialized).toEqual(registrationAuth);
  });

  test('Certificate should be stored along with private key', async () => {
    await registrar.register(DEFAULT_PUBLIC_GATEWAY);

    const key = await privateKeyStore.fetchNodeKey(idCertificate.getSerialNumber());
    expect(key.certificate.isEqual(idCertificate)).toBeTruthy();
  });

  test('Public gateway address should be stored in config', async () => {
    await registrar.register(DEFAULT_PUBLIC_GATEWAY);

    const config = Container.get(Config);
    await expect(config.get(PUBLIC_GATEWAY_ADDRESS)).resolves.toEqual(DEFAULT_PUBLIC_GATEWAY);
  });
});

describe('registerIfUnregistered', () => {
  let registerSpy: jest.SpyInstance;
  beforeEach(() => {
    registerSpy = jest.spyOn(registrar, 'register').mockResolvedValue();
  });
  afterEach(() => {
    registerSpy.mockRestore();
  });

  test('Registration should proceed if unregistered', async () => {
    await registrar.registerIfUnregistered();

    expect(registerSpy).toBeCalledWith(DEFAULT_PUBLIC_GATEWAY);
  });

  test('Registration should be skipped if already registered', async () => {
    const config = Container.get(Config);
    await config.set(PUBLIC_GATEWAY_ADDRESS, DEFAULT_PUBLIC_GATEWAY);

    await registrar.registerIfUnregistered();

    expect(registerSpy).toBeCalledTimes(0);
  });
});