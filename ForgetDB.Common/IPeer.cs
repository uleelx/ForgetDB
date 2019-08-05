using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace ForgetDB.Common
{
	public abstract class IPeer : IDisposable
	{
		public abstract void Send(byte[] data);
		public event OnReceiveEventHandler OnReceiveEvent;
		public delegate byte[] OnReceiveEventHandler(byte[] data);
		protected virtual byte[] OnReceive(byte[] data)
		{
			OnReceiveEventHandler handler = OnReceiveEvent;
			return handler?.Invoke(data);
		}

		public abstract void Dispose();
	}
}
