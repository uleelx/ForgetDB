using MessagePack;
using System;
using System.Collections.Generic;
using System.Text;

namespace ForgetDB
{
	public enum Seek
	{
		Earliest, Latest
	}

	[MessagePackObject]
	public class Packet
	{
		[Key(0)]
		public string Command { get; set; }
		[Key(1)]
		public string Key { get; set; }
		[Key(2)]
		public long ExpireAt { get; set; }
		[Key(3)]
		public string GroupId { get; set; }
		[Key(4)]
		public Seek Position { get; set; }
		[Key(5)]
		public long Step { get; set; }
		[Key(6)]
		public byte[] Value { get; set; }
	}
}
