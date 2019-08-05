using ForgetDB.Server;
using System;

namespace ForgetDB
{
	class Program
	{
		static void Main(string[] args)
		{
			using (ForgetServer server = new ForgetServer(9050))
			{
				Console.ReadLine();
			}
		}
	}
}
