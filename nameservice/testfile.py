import xmlrpc.client

proxy = xmlrpc.client.ServerProxy("http://localhost:5000/RPC2")

print(proxy.register_worker("sum", "192.168.0.10:6000"))
print(proxy.lookup_worker("sum"))
print(proxy.deregister_worker("192.168.0.10:6000"))
print(proxy.lookup_worker("sum"))
