using ForgetDB.Common;
using MessagePack;
using System;

namespace ForgetDB.Server
{
	public class ForgetServer : IDisposable
	{
		private static IPeer peer;
		public static IForgetDB db;

		private static int pushCount = 0;
		private static int pullCount = 0;
		private static double pushElapsedTime = 0;
		private static double pullElapsedTime = 0;

		private static readonly MessagePackSerializerOptions lz4Options = MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4BlockArray);

		public ForgetServer(int port, string dataBaseDir = "./db")
		{
			db = new LiteDBImp(dataBaseDir);
			peer = new NetMQPeer(port);
			peer.OnReceiveEvent += Peer_OnReceiveEvent;
		}

		private static byte[] Peer_OnReceiveEvent(byte[] data)
		{
			DateTime a = DateTime.Now;
			var packet = MessagePackSerializer.Deserialize<Packet>(data, lz4Options);
			byte[] response = null;
			switch (packet.Command)
			{
				case "PUSH":
					pushCount += 1;
					//Console.Write($"PUSH{pushCount}\t");
					db.Push(packet.Key, packet.Value, packet.ExpireAt);
					response = MessagePackSerializer.Serialize(new Packet
					{
						Command = "PUSH",
						Key = packet.Key,
						Value = null
					}, lz4Options);
					pushElapsedTime += DateTime.Now.Subtract(a).TotalSeconds;
					Console.WriteLine($"PUSH {pushElapsedTime}");
					break;
				case "PULL":
					pullCount += 1;
					//Console.Write($"PULL{pullCount}\t");
					byte[] value = db.Pull(packet.Key, packet.GroupId);
					response = MessagePackSerializer.Serialize(new Packet
					{
						Command = "PULL",
						Key = packet.Key,
						Value = value
					}, lz4Options);
					pullElapsedTime += DateTime.Now.Subtract(a).TotalSeconds;
					Console.WriteLine($"PULL {pullElapsedTime}");
					break;
			}
			return response;
		}

		public void Dispose()
		{
			db.Dispose();
			peer.Dispose();
		}
	}
}
