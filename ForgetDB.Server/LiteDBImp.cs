using LiteDB;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ForgetDB.Server
{
	public class Message
	{
		public long Id { get; set; }
		public string Key { get; set; }
		public byte[] Value { get; set; }
		public long ExpireAt { get; set; }
	}

	public class Group
	{
		public string GroupId { get; set; }
		public string Key { get; set; }
		public long NextId { get; set; }
	}

	public class LiteDBImp : IForgetDB
	{
		private string DataBaseDir;
		private ConcurrentDictionary<string, LiteDatabase> dbDict = new ConcurrentDictionary<string, LiteDatabase>();

		public LiteDBImp(string dataBaseDir = "./db")
		{
			if (!Directory.Exists(dataBaseDir))
			{
				Directory.CreateDirectory(dataBaseDir);
			}

			DataBaseDir = dataBaseDir;
		}

		public bool Push(string key, byte[] value, long expireAt = long.MaxValue)
		{
			LiteDatabase db = dbDict.GetOrAdd(key, k =>
			{
				return new LiteDatabase(Path.Combine(DataBaseDir, $"{k}.db"));
			});

			var message = db.GetCollection<Message>("messages");

			var newID = message.Insert(new Message
			{
				Key = key,
				Value = value,
				ExpireAt = expireAt
			});

			return true;
		}

		public byte[] Pull(string key, string groupId, Seek pos = Seek.Earliest, long step = 1)
		{
			LiteDatabase db = dbDict.GetOrAdd(key, k =>
			{
				return new LiteDatabase(Path.Combine(DataBaseDir, $"{k}.db"));
			});

			var offset = db.GetCollection<Group>("offsets");
			var message = db.GetCollection<Message>("messages");

			Group o = offset.FindById(groupId);

			long nextID = pos == Seek.Earliest ? (o == null ? 1 : o.NextId) : message.Max().AsInt64;

			Message m;
			long epochNow = Epoch();

			do
			{
				m = message.FindById(nextID);
				if (m == null || m.ExpireAt > epochNow)
				{
					break;
				}
				nextID++;
			} while (true);

			if (m == null)
			{
				return default(byte[]);
			}
			else
			{
				UpdateOffset(o);
				return m.Value;
			}

			void UpdateOffset(Group os)
			{
				os = os ?? new Group();
				os.GroupId = groupId;
				os.Key = key;
				os.NextId = m.Id + step;
				offset.Upsert(os);
			}

			long Epoch()
			{
				return DateTimeOffset.Now.ToUnixTimeSeconds();
			}
		}

		public void Dispose()
		{
			foreach (var db in dbDict.Values)
			{
				db.Dispose();
			}
		}
	}
}
