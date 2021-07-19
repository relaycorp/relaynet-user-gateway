import { CloseFrame, MockClient } from '@relaycorp/ws-mock';
import { EventEmitter } from 'events';
import { Duplex } from 'stream';
import WebSocket from 'ws';

import { mockLoggerToken, partialPinoLog } from '../testUtils/logging';
import { mockWebsocketStream } from '../testUtils/websocket';
import { makeWebSocketServer, WebSocketCode } from './websocket';

mockWebsocketStream();
const mockLogs = mockLoggerToken();

describe('WebSocket server configuration', () => {
  const serverResponseFrame = 'hello';

  test('Clients should not be tracked', () => {
    const wsServer = makeWebSocketServer(wsHandler);

    expect(wsServer.options.clientTracking).toBeFalsy();
  });

  test('No HTTP server should be created', () => {
    const wsServer = makeWebSocketServer(wsHandler);

    expect(wsServer.options.noServer).toBeTruthy();
  });

  test('CORS request should be blocked if auth is not required', async () => {
    const mockHandler = jest.fn();
    const wsServer = makeWebSocketServer(mockHandler);
    const mockClient = new MockClient(wsServer, { origin: 'https://example.com' });

    await mockClient.connect();
    await expect(mockClient.waitForPeerClosure()).resolves.toEqual({
      code: WebSocketCode.VIOLATED_POLICY,
      reason: 'CORS requests are disabled',
    });
    expect(mockLogs).toContainEqual(partialPinoLog('info', 'Refusing CORS request'));
    expect(mockHandler).not.toBeCalled();
  });

  test('CORS request should be allowed if auth is required and succeeds', async () => {
    const mockHandler = async (_: Duplex, socket: WebSocket) => {
      socket.close(1000);
    };
    const authToken = 'auth-token';
    const wsServer = makeWebSocketServer(mockHandler, authToken);
    const mockClient = new MockClient(
      wsServer,
      { origin: 'https://example.com' },
      `/?auth=${authToken}`,
    );

    await mockClient.connect();
    await expect(mockClient.waitForPeerClosure()).resolves.toEqual({
      code: 1000,
    });
  });

  test('Request should be refused if auth is required and invalid token is unset', async () => {
    const mockHandler = jest.fn();
    const wsServer = makeWebSocketServer(mockHandler, 'auth-token');
    const mockClient = new MockClient(wsServer);

    await mockClient.connect();
    await expect(mockClient.waitForPeerClosure()).resolves.toEqual({
      code: WebSocketCode.VIOLATED_POLICY,
      reason: 'Authentication is required',
    });
    expect(mockLogs).toContainEqual(partialPinoLog('info', 'Refusing unauthenticated request'));
    expect(mockHandler).not.toBeCalled();
  });

  test('Request should be refused if auth is required and invalid token is passed', async () => {
    const mockHandler = jest.fn();
    const wsServer = makeWebSocketServer(mockHandler, 'auth-token');
    const mockClient = new MockClient(wsServer, {}, '/?auth=invalid');

    await mockClient.connect();
    await expect(mockClient.waitForPeerClosure()).resolves.toEqual({
      code: WebSocketCode.VIOLATED_POLICY,
      reason: 'Authentication is required',
    });
    expect(mockLogs).toContainEqual(partialPinoLog('info', 'Refusing unauthenticated request'));
    expect(mockHandler).not.toBeCalled();
  });

  test('Specified handler should be called when connection is established', async () => {
    const handlerSpied = jest.fn().mockImplementation(wsHandler);
    const wsServer = makeWebSocketServer(handlerSpied);
    const headers = { foo: 'bar' };
    const mockClient = new MockClient(wsServer, headers);

    await mockClient.connect();
    await mockClient.send(serverResponseFrame);
    const response = await mockClient.receive();
    expect(response).toEqual(serverResponseFrame);
    await mockClient.waitForPeerClosure();

    expect(handlerSpied).toBeCalledWith(expect.any(Duplex), expect.any(EventEmitter), headers);
  });

  test('Unhandled exceptions should close the connection', async () => {
    const error = new Error('unhandled!');
    const mockHandler = async () => {
      throw error;
    };
    const wsServer = makeWebSocketServer(mockHandler);
    const mockClient = new MockClient(wsServer);

    await mockClient.connect();

    await expect(mockClient.waitForPeerClosure()).resolves.toEqual<CloseFrame>({
      code: WebSocketCode.SERVER_ERROR,
      reason: 'Internal server error',
    });
    expect(mockLogs).toContainEqual(
      partialPinoLog('error', 'Unhandled exception in WebSocket server handler', {
        err: expect.objectContaining({ message: error.message }),
      }),
    );
  });

  describe('Pings', () => {
    beforeEach(() => {
      jest.useFakeTimers();
    });
    afterEach(() => {
      jest.useRealTimers();
    });

    test('Connection should be terminated if client does not respond to pings', async () => {
      const wsServer = makeWebSocketServer(wsHandler);
      const mockClient = new MockClient(wsServer);

      await mockClient.connect();
      jest.advanceTimersByTime(31_000);

      expect(mockClient.wasConnectionClosed).toBeTrue();
    });

    test('Connection should be kept if client responds to pings', async () => {
      const wsServer = makeWebSocketServer(wsHandler);
      const mockClient = new MockClient(wsServer);

      await mockClient.connect();
      jest.advanceTimersByTime(31_000);

      expect(mockClient.wasConnectionClosed).toBeFalse();
    });
  });

  async function wsHandler(connectionStream: Duplex, socket: WebSocket): Promise<void> {
    socket.on('message', (message) => {
      connectionStream.write(message);
    });

    setImmediate(() => socket.close());
  }
});
