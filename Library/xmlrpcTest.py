import xmlrpc.client

def test_xmlrpc_write_outputs(server_url: str, name: str, value: int):
    try:
        proxy = xmlrpc.client.ServerProxy(server_url)
        result = proxy.write_output("0",name, value)

        return {"success": True, "result": result}
    except xmlrpc.client.Fault as fault:
        return {"success": False, "error": f"Server error: {fault}"}
    except xmlrpc.client.ProtocolError as error:
        return {"success": False, "error": f"Protocol error: {error}"}
    except Exception as e:
        return {"success": False, "error": f"Unexpected error: {e}"}


if __name__ == "__main__":
    server_url = "http://localhost:10001" 
    name = "ROB_Battery"
    value = 50

    response = test_xmlrpc_write_outputs(server_url, name, value)

    if response["success"]:
        print("Server response:", response["result"])
    else:
        print("Error:", response["error"])


    