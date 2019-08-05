using System;
using System.Threading;

namespace ForgetDB.Client.Test
{
	class Program
	{
		static void Main(string[] args)
		{
			Thread th1 = new Thread(new ThreadStart(() =>
			{
				using (var client = new ForgetClient<string>("localhost:9050", "Anonymous"))
				{
					var a = DateTime.Now;
					for (int i = 0; i < 1000; i++)
					{
						string v = $"world{i}";
						client.Produce("hello", v);
					}
					client.Produce("ok","YES");
					Console.WriteLine(DateTime.Now.Subtract(a).TotalSeconds);
				}
			}));

			Thread th2 = new Thread(new ThreadStart(() =>
			{
				using (var client = new ForgetClient<string>("localhost:9050", "Anonymous"))
				{
					client.Subscribe("hello");

					var a = DateTime.Now;
					for (int i = 0; i < 1000; i++)
					{
						string v = $"world{i}";
						string w = client.Consume();
						if (v != w)
						{
							Console.Write($"{v}|{w}\t");
						}
					}
					Console.WriteLine(DateTime.Now.Subtract(a).TotalSeconds);
					Console.WriteLine("Finished!");
					Console.WriteLine(client.Get("ok"));
				}
			}));

			Thread th3 = new Thread(new ThreadStart(() =>
			{
				using (var client = new ForgetClient<string>("localhost:9050", "Anonymous2"))
				{
					var a = DateTime.Now;
					for (int i = 0; i < 1000; i++)
					{
						string v = $"world{i}";
						string w = client.Consume("hello");
						if (v != w)
						{
							Console.Write($"{v}|{w}\t");
						}
					}
					Console.WriteLine(DateTime.Now.Subtract(a).TotalSeconds);
					Console.WriteLine("Finished!");
					Console.WriteLine(client.Get("ok"));
				}
			}));

			th1.Start();
			th2.Start();

			Console.ReadLine();
			th3.Start();

			Console.ReadLine();
		}
	}
}
