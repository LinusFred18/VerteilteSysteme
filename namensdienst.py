from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

class Namensdienst:
    def __init__(self):
        self.worker_registry = {}

    def register_worker(self, typ: str, address: str) -> str:
        if typ and address:
            self.worker_registry[typ] = address
            return "Erfolgreich registriert"
        return "Registrierung fehlgeschlagen!"

    def lookup_worker(self, typ: str):
        address = self.worker_registry.get(typ)
        if address:
            return address
        return "Es konnte kein Worker dieses Typs gefunden werden!"

    def deregister_worker(self, address: str) -> str:
        found = False
        for typ, addr in list(self.worker_registry.items()):
            if addr == address:
                del self.worker_registry[typ]
                found = True
        return "Erfolgreich deregistriert" if found else "Deregistrierung fehlgeschlagen!"

# Eingrenzung des Pfads auf /RPC2 (optional, sicherer)
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

if __name__ == "__main__":
    server = SimpleXMLRPCServer(("0.0.0.0", 5000), requestHandler=RequestHandler, allow_none=True)
    server.register_introspection_functions()

    ns = Namensdienst()

    server.register_instance(ns)

    print("[Namensdienst] Läuft als RPC-Server auf Port 5000...")
    server.serve_forever()


