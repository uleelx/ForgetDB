using DBreeze;
using DBreeze.Utils;
using System;
using System.Collections.Generic;
using System.Text;

namespace ForgetDB.Server
{
	public class DBreezeImp : IForgetDB
	{
		private static DBreezeEngine engine = null;

		public DBreezeImp(string dataBaseDir = "./db")
		{
			if (engine == null)
				engine = new DBreezeEngine(dataBaseDir);
		}

		public bool Push(string key, byte[] value, long expireAt = long.MaxValue)
		{
			try
			{
				using (var trans = engine.GetTransaction())
				{
					//trans.SynchronizeTables(key);

					trans.Technical_SetTable_OverwriteIsNotAllowed(key);

					long id = trans.ObjectGetNewIdentity<long>(key);
					trans.InsertPart<long, byte[]>(key, id, expireAt.To_8_bytes_array_BigEndian(), 0);
					trans.InsertPart<long, byte[]>(key, id, value, 8);
					trans.Commit();
				}
				return true;
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex);
				return false;
			}
		}

		public byte[] Pull(string key, string groupId, Seek pos = Seek.Earliest, long step = 1)
		{
			try
			{
				using (var trans = engine.GetTransaction())
				{
					//trans.SynchronizeTables(key, groupId);

					var groupRow = trans.Select<string, long>(groupId, key, true);

					long id = 1;

					if (groupRow.Exists)
					{
						if (pos == Seek.Earliest)
						{
							id = groupRow.Value;
						}
						else
						{
							var maxTopicRow = trans.Max<long, byte[]>(key);
							if (maxTopicRow.Exists)
								id = maxTopicRow.Key;
						}
					}

					long epochNow = Epoch();

					foreach (var topicRow in trans.SelectForwardStartFrom<long, byte[]>(key, id, true))
					{
						if (topicRow.Exists)
						{
							if (topicRow.GetValuePart(0, 8).To_Int64_BigEndian() > epochNow)
							{
								trans.Insert(groupId, key, id + step);
								trans.Commit();

								return topicRow.GetValuePart(8);
							}
						}
						else
						{
							return default(byte[]);
						}
					}
					return default(byte[]);
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex);
				return default(byte[]);
			}

			long Epoch()
			{
				return DateTimeOffset.Now.ToUnixTimeSeconds();
			}
		}

		public void Dispose()
		{
			engine?.Dispose();
		}
	}
}
