using System;
using System.Collections.Generic;
using System.Text;

namespace ForgetDB.Server
{
	public interface IForgetDB : IDisposable
	{
		bool Push(string key, byte[] value, long expireAt = long.MaxValue);
		byte[] Pull(string key, string groupId, Seek pos = Seek.Earliest, long step = 1);
	}
}
