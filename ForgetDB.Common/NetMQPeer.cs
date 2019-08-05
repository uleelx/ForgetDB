using NetMQ;
using NetMQ.Sockets;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;

namespace ForgetDB.Common
{
	public class NetMQPeer : IPeer
	{
		private DealerSocket dealerSocket;
		private RouterSocket routerSocket;

		NetMQPoller poller;

		Thread longRunThread;

		ConcurrentQueue<byte[]> sendQueue;

		public NetMQPeer(int bindPort)
		{
			routerSocket = new RouterSocket();
			routerSocket.Bind($"tcp://*:{bindPort}");

			longRunThread = new Thread(new ThreadStart(ServerLoop));
			longRunThread.IsBackground = true;
			longRunThread.Start();
		}

		public NetMQPeer(string address)
		{
			dealerSocket = new DealerSocket();
			dealerSocket.Options.SendHighWatermark = 0;
			dealerSocket.Options.ReceiveHighWatermark = 0;
			dealerSocket.Connect($"tcp://{address}");
			dealerSocket.ReceiveReady += DealerSocket_ReceiveReady;
			dealerSocket.SendReady += DealerSocket_SendReady;

			sendQueue = new ConcurrentQueue<byte[]>();

			longRunThread = new Thread(new ThreadStart(ClientLoop));

			longRunThread.IsBackground = true;
			longRunThread.Start();
		}

		private void DealerSocket_SendReady(object sender, NetMQSocketEventArgs e)
		{
			while (!sendQueue.IsEmpty)
			{
				if (sendQueue.TryDequeue(out byte[] data))
				{
					var messageToServer = new NetMQMessage();
					messageToServer.AppendEmptyFrame();
					messageToServer.Append(data);
					dealerSocket.SendMultipartMessage(messageToServer);
				}
			}
		}

		private void DealerSocket_ReceiveReady(object sender, NetMQSocketEventArgs e)
		{
			var msg = e.Socket.ReceiveMultipartMessage();
			if (msg.FrameCount == 2)
			{
				byte[] result = msg[1].ToByteArray();
				OnReceive(result);
			}
		}

		private void ClientLoop()
		{
			poller = new NetMQPoller();
			poller.Add(dealerSocket);
			poller.Run();
		}

		private void ServerLoop()
		{
			while (true)
			{
				var clientMessage = routerSocket.ReceiveMultipartMessage();
				if (clientMessage.FrameCount == 3)
				{
					var clientAddress = clientMessage[0];
					var sendData = OnReceive(clientMessage[2].ToByteArray());
					var messageToClient = new NetMQMessage();
					messageToClient.Append(clientAddress);
					messageToClient.AppendEmptyFrame();
					messageToClient.Append(sendData);
					routerSocket.SendMultipartMessage(messageToClient);
				}
			}
		}

		public override void Dispose()
		{
			poller?.Dispose();
			dealerSocket?.Dispose();
			routerSocket?.Dispose();
		}

		public override void Send(byte[] data)
		{
			sendQueue.Enqueue(data);
		}
	}
}
