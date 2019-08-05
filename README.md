# ForgetDB
A Kafka-like server and client for .NET using LiteDB as persistent storage

Quick Look
==========
```csharp
// Server

using (ForgetServer server = new ForgetServer(9050))
{
	Console.ReadLine();
}
```

```csharp
// Client - Produce
using (var client = new ForgetClient<string>("localhost:9050", "Anonymous"))
{
	for (int i = 0; i < 1000; i++)
	{
		string v = $"world{i}";
		client.Produce("hello", v);
	}
	client.Produce("ok","YES");
}

// Client - Consume
using (var client = new ForgetClient<string>("localhost:9050", "Anonymous"))
{
	client.Subscribe("hello");

	for (int i = 0; i < 1000; i++)
	{
		string v = $"world{i}";
		string w = client.Consume();
		if (v != w)
		{
			Console.Write($"{v}|{w}\t");
		}
	}
}
```