using ForgetDB.Common;
using MessagePack;
using System;
using System.Collections.Concurrent;
using System.Net;
using System.Threading;

namespace ForgetDB.Client
{
	public class ForgetClient<T> : IDisposable
	{
		private IPeer peer;

		private ManualResetEvent hasNewItem = new ManualResetEvent(false);

		private ConcurrentQueue<T> itemQueue = new ConcurrentQueue<T>();

		private string GroupId;

		private string Topic;

		public ForgetClient(string serverIp, string groupId)
		{
			GroupId = groupId;
			peer = new NetMQPeer(serverIp);
			peer.OnReceiveEvent += Peer_OnReceiveEvent;
		}

		private byte[] Peer_OnReceiveEvent(byte[] data)
		{
			var packet = LZ4MessagePackSerializer.Deserialize<Packet>(data);
			if (packet.Value != null)
			{
				itemQueue.Enqueue(LZ4MessagePackSerializer.Deserialize<T>(packet.Value));
				hasNewItem.Set();
			}
			return null;
		}

		public void Produce(string key, T value, long expireAt = long.MaxValue)
		{
			byte[] packet = LZ4MessagePackSerializer.Serialize(new Packet
			{
				Command = "PUSH",
				Key = key,
				Value = LZ4MessagePackSerializer.Serialize(value),
				ExpireAt = expireAt
			});

			peer.Send(packet);
		}

		public T Consume(string key, Seek pos = Seek.Earliest, long step = 1)
		{
			byte[] packet = LZ4MessagePackSerializer.Serialize(new Packet
			{
				Command = "PULL",
				Key = key,
				GroupId = GroupId,
				Position = pos,
				Step = step
			});

			while (itemQueue.IsEmpty)
			{
				peer.Send(packet);
				WaitHandle.WaitAny(new WaitHandle[] { hasNewItem }, 1000);
			}
			itemQueue.TryDequeue(out T result);
			if (itemQueue.IsEmpty)
			{
				hasNewItem.Reset();
			}
			return result;
		}

		public T Consume()
		{
			return Consume(Topic);
		}

		public void Subscribe(string topic)
		{
			Topic = topic;
		}

		public void Set(string key, T value, long expireAt = long.MaxValue)
		{
			Produce(key, value, expireAt);
		}

		public T Get(string key)
		{
			return Consume(key, Seek.Latest, 0);
		}

		public void Dispose()
		{
			peer.Dispose();
		}
	}
}
