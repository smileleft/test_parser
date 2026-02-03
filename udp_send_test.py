import socket, json

payload = {
  "timestamp":"2026-01-23T14:23:25.287",
  "stream":2,
  "function":49,
  "wbit":True,
  "deviceId":1,
  "systemBytes":"0x6E881309",
  "ptype":0,
  "stype":0,
  "body":{
    "type":"L",
    "value":[
      {"type":"U2","value":[0]},
      {"type":"A","value":""},
      {"type":"A","value":"TRANSFER"},
      {"type":"L","value":[
        {"type":"L","value":[
          {"type":"A","value":"COMMANDINFO"},
          {"type":"L","value":[
            {"type":"L","value":[{"type":"A","value":"COMMANDID"},{"type":"A","value":"EXER_001_2026012314232539"}]},
            {"type":"L","value":[{"type":"A","value":"PRIORITY"},{"type":"U2","value":[55]}]}
          ]}
        ]},
        {"type":"L","value":[
          {"type":"A","value":"TRANSFERINFO"},
          {"type":"L","value":[
            {"type":"L","value":[{"type":"A","value":"CARRIERID"},{"type":"A","value":"EXER_001"}]},
            {"type":"L","value":[{"type":"A","value":"SOURCE"},{"type":"A","value":"E01_0285"}]},
            {"type":"L","value":[{"type":"A","value":"DEST"},{"type":"A","value":"E02_0318"}]},
            {"type":"L","value":[{"type":"A","value":"SOURCETYPE"},{"type":"A","value":"1"}]},
            {"type":"L","value":[{"type":"A","value":"DESTTYPE"},{"type":"A","value":"1"}]}
          ]}
        ]}
      ]}
    ]
  }
}

data = json.dumps(payload).encode("utf-8")
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.sendto(data, ("127.0.0.1", 5000))
print("sent")
