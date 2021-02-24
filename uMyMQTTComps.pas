unit uMyMQTTComps;

interface

uses
  SysUtils, Classes, System.Generics.Collections, blcksock, ssl_openssl, ssl_openssl_lib,

  uMyMQTT, uSimpleThreadTimer, uMyMQTTReadThread;

(* Todo
  Finish Retain
*)
(* Web Sites
  http://www.alphaworks.ibm.com/tech/rsmb
  http://www.mqtt.org

  Permission to copy and display the MQ Telemetry Transport specification (the
  "Specification"), in any medium without fee or royalty is hereby granted by Eurotech
  and International Business Machines Corporation (IBM) (collectively, the "Authors"),
  provided that you include the following on ALL copies of the Specification, or portions
  thereof, that you make:
  A link or URL to the Specification at one of
  1. the Authors' websites.
  2. The copyright notice as shown in the Specification.

  The Authors each agree to grant you a royalty-free license, under reasonable,
  non-discriminatory terms and conditions to their respective patents that they deem
  necessary to implement the Specification. THE SPECIFICATION IS PROVIDED "AS IS,"
  AND THE AUTHORS MAKE NO REPRESENTATIONS OR WARRANTIES, EXPRESS OR
  IMPLIED, INCLUDING, BUT NOT LIMITED TO, WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE, NON-INFRINGEMENT, OR TITLE; THAT THE
  CONTENTS OF THE SPECIFICATION ARE SUITABLE FOR ANY PURPOSE; NOR THAT THE
  IMPLEMENTATION OF SUCH CONTENTS WILL NOT INFRINGE ANY THIRD PARTY
  PATENTS, COPYRIGHTS, TRADEMARKS OR OTHER RIGHTS. THE AUTHORS WILL NOT
  BE LIABLE FOR ANY DIRECT, INDIRECT, SPECIAL, INCIDENTAL OR CONSEQUENTIAL
  DAMAGES ARISING OUT OF OR RELATING TO ANY USE OR DISTRIBUTION OF THE
  SPECIFICATION *)

const
  MinVersion = 3;

type
  TMyMQTTClient = class;
  TMQTTPacketStore = class;
  TMQTTMessageStore = class;

  TMQTTPacket = class
    ID: Word;
    Stamp: TDateTime;
    Counter: cardinal;
    Retries: integer;
    Publishing: Boolean;
    Msg: TMemoryStream;
    procedure Assign(From: TMQTTPacket);
    constructor Create;
    destructor Destroy; override;
  end;

  TMQTTMessage = class
    ID: Word;
    Stamp: TDateTime;
    LastUsed: TDateTime;
    Qos: TMQTTQOSType;
    Retained: Boolean;
    Counter: cardinal;
    Retries: integer;
    Topic: UTF8String;
    Message: string;
    procedure Assign(From: TMQTTMessage);
    constructor Create;
    destructor Destroy; override;
  end;

  TMQTTSession = class
    ClientID: UTF8String;
    Stamp: TDateTime;
    InFlight: TMQTTPacketStore;
    Releasables: TMQTTMessageStore;
    constructor Create;
    destructor Destroy; override;
  end;

  TMQTTSessionStore = class
    List: TList;
    Stamp: TDateTime;
    function GetItem(Index: integer): TMQTTSession;
    procedure SetItem(Index: integer; const Value: TMQTTSession);
    property Items[Index: integer]: TMQTTSession read GetItem
      write SetItem; default;
    function Count: integer;
    procedure Clear;
    function GetSession(ClientID: UTF8String): TMQTTSession;
    procedure StoreSession(ClientID: UTF8String;
      aClient: TMyMQTTClient); overload;
    procedure DeleteSession(ClientID: UTF8String);
    procedure RestoreSession(ClientID: UTF8String;
      aClient: TMyMQTTClient); overload;
    constructor Create;
    destructor Destroy; override;
  end;

  TMQTTPacketStore = class
    List: TObjectList<TMQTTPacket>;
    Stamp: TDateTime;
    function GetItem(Index: integer): TMQTTPacket;
    procedure SetItem(Index: integer; const Value: TMQTTPacket);
    property Items[Index: integer]: TMQTTPacket read GetItem
      write SetItem; default;
    function Count: integer;
    procedure Clear;
    procedure Assign(From: TMQTTPacketStore);
    function AddPacket(anID: Word; aMsg: TMemoryStream; aRetry: cardinal;
      aCount: cardinal): TMQTTPacket;
    procedure DelPacket(anID: Word);
    function GetPacket(anID: Word): TMQTTPacket;
    procedure Remove(aPacket: TMQTTPacket);
    constructor Create;
    destructor Destroy; override;
  end;

  TMQTTMessageStore = class
    List: TObjectList<TMQTTMessage>;
    Stamp: TDateTime;
    function GetItem(Index: integer): TMQTTMessage;
    procedure SetItem(Index: integer; const Value: TMQTTMessage);
    property Items[Index: integer]: TMQTTMessage read GetItem
      write SetItem; default;
    function Count: integer;
    procedure Clear;
    procedure Assign(From: TMQTTMessageStore);
    function AddMsg(anID: Word; aTopic: UTF8String; aMessage: UTF8String;
      aQos: TMQTTQOSType; aRetry: cardinal; aCount: cardinal;
      aRetained: Boolean = false): TMQTTMessage;
    procedure DelMsg(anID: Word);
    function GetMsg(anID: Word): TMQTTMessage;
    procedure Remove(aMsg: TMQTTMessage);
    constructor Create;
    destructor Destroy; override;
  end;

  TMyMQTTClient = class(TComponent)
  private
    FUsername, FPassword: UTF8String;
    FMessageID: Word;
    FHost: string;
    FPort: integer;
    FOnline: Boolean;
    FGraceful: Boolean;
    FOnMon: TMQTTMonEvent;
    FOnOnline: TNotifyEvent;
    FOnOffline: TMQTTDisconnectEvent;
    FOnMsg: TMQTTMsgEvent;
    FOnFailure: TMQTTFailureEvent;
    FLocalBounce: Boolean;
    FAutoSubscribe: Boolean;
    FOnClientID: TMQTTClientIDEvent;
    FBroker: Boolean; // non standard

    FPingTimer, FWillTimer: TSimpleThreadTimer;
    FRecvThread: TMQTTReadThread;
    FIsConnected: Boolean;
    FIsClosed: Boolean;
    FPingInterval: Integer;
    // ssl
    FUseSSL: Boolean;
    FSSLCertFile: string;
    FSSLKeyFile: string;
    FSSLRootCertFile: string;
    FPassThrough: Boolean;
    FSSLLibPath: string;

    Link: TTCPBlockSocket;


    Parser: TMQTTParser;
    InFlight: TMQTTPacketStore;
    Releasables: TMQTTMessageStore;
    Subscriptions: TStringList;
    procedure onData(data: TStream);
    procedure DoSend(Sender: TObject; anID: Word; aRetry: integer;
      aStream: TMemoryStream);
    procedure SendData(data: TBytes); overload;
    procedure SendData(stream: TStream); overload;
    procedure RxConnAck(Sender: TObject; aCode: byte);
    procedure RxSubAck(Sender: TObject; anID: Word;
      Qoss: array of TMQTTQOSType);
    procedure RxPubAck(Sender: TObject; anID: Word);
    procedure RxPubRec(Sender: TObject; anID: Word);
    procedure RxPubRel(Sender: TObject; anID: Word);
    procedure RxPubComp(Sender: TObject; anID: Word);
    procedure RxPublish(Sender: TObject; anID: Word; aTopic: UTF8String;
      aMessage: string);
    procedure RxUnsubAck(Sender: TObject; anID: Word);
    // procedure LinkConnected (Sender: TObject; ErrCode: Word);
    // procedure LinkClosed (Sender: TObject; ErrCode: Word);
    procedure AfterConnected(Sender: TObject);
    procedure BeforeDisconnected(Sender: TObject);
    function GetClientID: UTF8String;
    procedure SetClientID(const Value: UTF8String);
    function GetKeepAlive: Word;
    procedure SetKeepAlive(const Value: Word);
    function GetMaxRetries: integer;
    procedure SetMaxRetries(const Value: integer);
    function GetRetryTime: cardinal;
    procedure SetRetryTime(const Value: cardinal);
    function GetClean: Boolean;
    procedure SetClean(const Value: Boolean);
    function GetPassword: UTF8String;
    function GetUsername: UTF8String;
    procedure SetPassword(const Value: UTF8String);
    procedure SetUsername(const Value: UTF8String);
    procedure PingTimerProc(Sender: TObject);
    procedure WillTimerProc(Sender: TObject);
  public
    function NextMessageID: Word;
    procedure Subscribe(aTopic: string; aQos: TMQTTQOSType); overload;
    procedure Subscribe(Topics: TStringList); overload;
    procedure Unsubscribe(aTopic: string); overload;
    procedure Unsubscribe(Topics: TStringList); overload;
    procedure Ping;
    procedure Publish(aTopic: UTF8String; aMessage: string; aQos: TMQTTQOSType;
      aRetain: Boolean = false);
    procedure SetWill(aTopic, aMessage: UTF8String; aQos: TMQTTQOSType;
      aRetain: Boolean = false);
    procedure Mon(aStr: string);

    constructor Create(anOwner: TComponent); override;
    destructor Destroy; override;

    procedure Connect;
    procedure Disconnect;
    procedure StartPing;
    procedure StopPing;

    property Connected: Boolean read FIsConnected;
    property Online: Boolean read FOnline;
  published
    property ClientID: UTF8String read GetClientID write SetClientID;
    property KeepAlive: Word read GetKeepAlive write SetKeepAlive;
    property MaxRetries: integer read GetMaxRetries write SetMaxRetries;
    property RetryTime: cardinal read GetRetryTime write SetRetryTime;
    property Clean: Boolean read GetClean write SetClean;
    property Broker: Boolean read FBroker write FBroker; // no standard
    property AutoSubscribe: Boolean read FAutoSubscribe write FAutoSubscribe;
    property Username: UTF8String read GetUsername write SetUsername;
    property Password: UTF8String read GetPassword write SetPassword;
    property Host: string read FHost write FHost;
    property Port: integer read FPort write FPort;
    property LocalBounce: Boolean read FLocalBounce write FLocalBounce;
    property OnClientID: TMQTTClientIDEvent read FOnClientID write FOnClientID;
    property OnMon: TMQTTMonEvent read FOnMon write FOnMon;
    property OnOnline: TNotifyEvent read FOnOnline write FOnOnline;
    property OnOffline: TMQTTDisconnectEvent read FOnOffline write FOnOffline;
    property OnFailure: TMQTTFailureEvent read FOnFailure write FOnFailure;
    property OnMsg: TMQTTMsgEvent read FOnMsg write FOnMsg;
    property PingInterval: Integer read FPingInterval write FPingInterval;
    property UseSSL: Boolean read FUseSSL write FUseSSL;
    property PassThrough: Boolean read FPassThrough write FPassThrough;
    property SSLLibPath: string read FSSLLibPath write FSSLLibPath;

  end;

procedure Register;
function SubTopics(aTopic: UTF8String): TStringList;
function IsSubscribed(aSubscription, aTopic: UTF8String): Boolean;

implementation

procedure Register;
begin
  RegisterComponents('MQTT', [TMyMQTTClient]);
end;


function SubTopics(aTopic: UTF8String): TStringList;
var
  i: integer;
begin
  result := TStringList.Create;
  result.Add('');
  for i := 1 to length(aTopic) do
  begin
    if aTopic[i] = '/' then
      result.Add('')
    else
      result[result.Count - 1] := result[result.Count - 1] + Char(aTopic[i]);
  end;
end;

function IsSubscribed(aSubscription, aTopic: UTF8String): Boolean;
var
  s, t: TStringList;
  i: integer;
  MultiLevel: Boolean;
begin
  s := SubTopics(aSubscription);
  t := SubTopics(aTopic);
  MultiLevel := (s[s.Count - 1] = '#'); // last field is #
  if not MultiLevel then
    result := (s.Count = t.Count)
  else
    result := (s.Count <= t.Count + 1);
  if result then
  begin
    for i := 0 to s.Count - 1 do
    begin
      if (i >= t.Count) then
        result := MultiLevel
      else if (i = s.Count - 1) and (s[i] = '#') then
        break
      else if s[i] = '+' then
        continue // they match
      else
        result := result and (s[i] = t[i]);
      if not result then
        break;
    end;
  end;
  s.Free;
  t.Free;
end;

procedure SetDup(aStream: TMemoryStream; aState: Boolean);
var
  x: byte;
begin
  if aStream.Size = 0 then
    exit;
//  aStream.Seek(0, soFromBeginning);
  aStream.Position := 0;
  aStream.Read(x, 1);
  x := (x and $F7) or (ord(aState) * $08);
//  aStream.Seek(0, soFromBeginning);
  aStream.Position := 0;
  aStream.Write(x, 1);
end;

{ TMQTTClient }

procedure TMyMQTTClient.Connect;
begin

  if Link = nil then
  begin
    if FUseSSL then
    begin
      _SslLibPath := FSSLLibPath;
      if not IsSSLloaded then
        InitSSLInterface;
      Link := TTCPBlockSocket.CreateWithSSL(TSSLOpenSSL);
    end else
    begin
      Link := TTCPBlockSocket.Create;
    end;

    Link.SizeRecvBuffer := c64k;
    Link.SizeSendBuffer := c64k;
    Link.SetSendTimeout(5000);
    Link.SetRecvTimeout(5000);
    Link.SetTimeout(5000);
    Link.ConnectionTimeout := 5000;
//    Link.OnAfterConnect := MyConnected;
  end
  else
  begin
    Link.CloseSocket;
  end;

  Link.Connect(Self.Host, IntToStr(Self.FPort));
  if Link.LastError = 0 then
  begin
    if FUseSSL then
      Link.SSLDoConnect;
    if Link.LastError = 0 then
      AfterConnected(Link);
  end;

end;

constructor TMyMQTTClient.Create(anOwner: TComponent);
begin
  inherited;
  FIsConnected := False;
  FIsClosed := True;
  FHost := '';
  FUsername := '';
  FPassword := '';
  FPort := 1883;
  FGraceful := false;
  FOnline := false;
  FBroker := false; // non standard
  FLocalBounce := false;
  FAutoSubscribe := false;
  FMessageID := 0;

  FPingInterval := 30;

  Subscriptions := TStringList.Create;
  Releasables := TMQTTMessageStore.Create;
  Parser := TMQTTParser.Create;
  Parser.OnSend := DoSend;
  Parser.OnConnAck := RxConnAck;
  Parser.OnPublish := RxPublish;
  Parser.OnSubAck := RxSubAck;
  Parser.OnUnsubAck := RxUnsubAck;
  Parser.OnPubAck := RxPubAck;
  Parser.OnPubRel := RxPubRel;
  Parser.OnPubRec := RxPubRec;
  Parser.OnPubComp := RxPubComp;
  Parser.KeepAlive := 60;

  InFlight := TMQTTPacketStore.Create;

end;

destructor TMyMQTTClient.Destroy;
begin


  if not FIsClosed then
    Disconnect;


  if Releasables <> nil then
  begin
    Releasables.Clear;
    Releasables.Free;
  end;
  if Subscriptions <> nil then
    Subscriptions.Free;
  if InFlight <> nil then
  begin
    InFlight.Clear;
    InFlight.Free;
  end;

  if Parser <> nil then
    Parser.Free;

  if Link <> nil then
    Link.Free;


  inherited;
end;

procedure TMyMQTTClient.Disconnect;
begin

  FGraceful := True;
  BeforeDisconnected(Link);

  if Link <> nil then
  begin
    try
      if FIsConnected then
      begin
        Parser.SendDisconnect;
      end;
    finally
      FIsConnected := false;
    end;
    Link.CloseSocket;
  end;

  FIsClosed := True;
end;

procedure TMyMQTTClient.RxConnAck(Sender: TObject; aCode: byte);
var
  i: integer;
  x: cardinal;
begin
  Mon ('Connection ' + codenames(aCode));
  if aCode = rcACCEPTED then
  begin
    FOnline := True;
    FGraceful := false;
    // SetTimer (Timers, 3, 100, nil);  // start retry counters
    // SetMyTimer(3, 100);
    if Assigned(FOnOnline) then
      FOnOnline(Self);
    if (FAutoSubscribe) and (Subscriptions.Count > 0) then
    begin
      for i := 0 to Subscriptions.Count - 1 do
      begin
        // x := cardinal (Subscriptions.Objects[i]) and $03;
        x := Subscriptions.ValueFromIndex[i].ToInteger and $03;
        // Parser.SendSubscribe (NextMessageID, UTF8String (Subscriptions[i]), TMQTTQOSType (x));
        Parser.SendSubscribe(NextMessageID, UTF8String(Subscriptions.Names[i]),
          TMQTTQOSType(x));
      end;
    end;
  end

end;

// publishing
procedure TMyMQTTClient.RxPublish(Sender: TObject; anID: Word;
  aTopic: UTF8String; aMessage: string);
var
  aMsg: TMQTTMessage;
begin
  case Parser.RxQos of
    qtAT_MOST_ONCE:
      if Assigned(FOnMsg) then
        FOnMsg(Self, aTopic, aMessage, Parser.RxQos, Parser.RxRetain);
    qtAT_LEAST_ONCE:
      begin
        Parser.SendPubAck(anID);
        if Assigned(FOnMsg) then
          FOnMsg(Self, aTopic, aMessage, Parser.RxQos, Parser.RxRetain);
      end;
    qtEXACTLY_ONCE:
      begin
        Parser.SendPubRec(anID);
        aMsg := Releasables.GetMsg(anID);
        if aMsg = nil then
        begin
          Releasables.AddMsg(anID, aTopic, aMessage, Parser.RxQos, 0, 0,
            Parser.RxRetain);
          Mon('Message ' + IntToStr(anID) + ' stored and idle.');
        end
        else
          Mon('Message ' + IntToStr(anID) + ' already stored.');
      end;
  end;
end;

procedure TMyMQTTClient.RxPubAck(Sender: TObject; anID: Word);
begin
  InFlight.DelPacket(anID);
  Mon('ACK Message ' + IntToStr(anID) + ' disposed of.');
end;

procedure TMyMQTTClient.RxPubComp(Sender: TObject; anID: Word);
begin
  InFlight.DelPacket(anID);
  Mon('COMP Message ' + IntToStr(anID) + ' disposed of.');
end;

procedure TMyMQTTClient.RxPubRec(Sender: TObject; anID: Word);
var
  aPacket: TMQTTPacket;
begin
  aPacket := InFlight.GetPacket(anID);
  if aPacket <> nil then
  begin
    aPacket.Counter := Parser.RetryTime;
    if aPacket.Publishing then
    begin
      aPacket.Publishing := false;
      Mon('REC Message ' + IntToStr(anID) + ' recorded.');
    end
    else
      Mon('REC Message ' + IntToStr(anID) + ' already recorded.');
  end
  else
    Mon('REC Message ' + IntToStr(anID) + ' not found.');
  Parser.SendPubRel(anID);
end;

procedure TMyMQTTClient.RxPubRel(Sender: TObject; anID: Word);
var
  aMsg: TMQTTMessage;
begin
  aMsg := Releasables.GetMsg(anID);
  if aMsg <> nil then
  begin
    Mon('REL Message ' + IntToStr(anID) + ' publishing @ ' +
      QOSNames[aMsg.Qos]);
    if Assigned(FOnMsg) then
      FOnMsg(Self, aMsg.Topic, aMsg.Message, aMsg.Qos, aMsg.Retained);
    Releasables.Remove(aMsg);
//    if aMsg <> nil then
//      aMsg.Free;
    Mon('REL Message ' + IntToStr(anID) + ' removed from storage.');
  end
  else
    Mon('REL Message ' + IntToStr(anID) +
      ' has been already removed from storage.');
  Parser.SendPubComp(anID);
end;

procedure TMyMQTTClient.SendData(data: TBytes);
begin
  if FIsConnected then
  begin
    try
      if Link <> nil then
      begin
        Link.SendBuffer(@data[0], length(data));
      end;
    except
      FIsConnected := false;
      Disconnect;
    end;
  end;
end;

procedure TMyMQTTClient.SendData(stream: TStream);
var data: TBytes;
    i: integer;
begin
//  stream.Seek(0, soFromBeginning);
  stream.Position := 0;
  if FIsConnected then
  begin
    try
      if Link <> nil then
      begin
        setlength(data, stream.Size);
        stream.Read(data, stream.Size);
        i := Link.SendBuffer(@data[0], length(data));
        Mon('send data size: ' + i.ToString);
        if (i = 0) or (Link.LastError <> 0) then
        begin
          FIsConnected := false;
          Disconnect;
        end;
      end;
    except
      FIsConnected := false;
      Disconnect;
    end;
  end;

end;

procedure TMyMQTTClient.SetClean(const Value: Boolean);
begin
  Parser.Clean := Value;
end;

procedure TMyMQTTClient.SetClientID(const Value: UTF8String);
begin
  Parser.ClientID := Value;
end;

procedure TMyMQTTClient.SetKeepAlive(const Value: Word);
begin
  Parser.KeepAlive := Value;
end;

procedure TMyMQTTClient.SetMaxRetries(const Value: integer);
begin
  Parser.MaxRetries := Value;
end;

procedure TMyMQTTClient.SetPassword(const Value: UTF8String);
begin
  Parser.Password := Value;
end;

procedure TMyMQTTClient.SetRetryTime(const Value: cardinal);
begin
  Parser.RetryTime := Value;
end;

procedure TMyMQTTClient.SetUsername(const Value: UTF8String);
begin
  Parser.Username := Value;
end;

procedure TMyMQTTClient.SetWill(aTopic, aMessage: UTF8String;
  aQos: TMQTTQOSType; aRetain: Boolean);
begin
  Parser.SetWill(aTopic, aMessage, aQos, aRetain);
end;

procedure TMyMQTTClient.StartPing;
begin
  FPingTimer := TSimpleThreadTimer.Create;
  FPingTimer.FreeOnTerminate := True;
  FPingTimer.OnTimer := PingTimerProc;
  FPingTimer.Interval := 1000 * FPingInterval;
  FPingTimer.Start;
end;

procedure TMyMQTTClient.StopPing;
begin
  if FPingTimer <> nil then
  begin
    FPingTimer.Terminate;
    FPingTimer.WaitFor;
    FPingTimer.Free;
  end;
end;

procedure TMyMQTTClient.Subscribe(Topics: TStringList);
var
  j: integer;
  i, x: cardinal;
  anID: Word;
  found: Boolean;
begin
  if Topics = nil then
    exit;
  anID := NextMessageID;
  for i := 0 to Topics.Count - 1 do
  begin
    found := false;
    // 255 denotes acked
    if i > 254 then
      x := (cardinal(Topics.Objects[i]) and $03)
    else
      x := (cardinal(Topics.Objects[i]) and $03) + (anID shl 16) + (i shl 8);
    for j := 0 to Subscriptions.Count - 1 do
      // if Subscriptions[j] = Topics[i] then
      if Subscriptions.Names[j] = Topics[i] then
      begin
        found := True;
        // Subscriptions.Objects[j] := TObject (x);
        Subscriptions.ValueFromIndex[j] := x.ToString;
        break;
      end;
    if not found then
      Subscriptions.Append(Topics[i] + '=' + x.ToString);
    // Subscriptions.AddObject (Topics[i], TObject (x));
  end;
//  StopPing;
  Parser.SendSubscribe(anID, Topics);
//  StartPing;
end;

procedure TMyMQTTClient.Subscribe(aTopic: string; aQos: TMQTTQOSType);
var
  i: integer;
  x: cardinal;
  found: Boolean;
  anID: Word;
begin

  if aTopic = '' then
    exit;
  found := false;
  anID := NextMessageID;
  x := ord(aQos) + (anID shl 16);
  for i := 0 to Subscriptions.Count - 1 do
    if Subscriptions.Names[i] = aTopic then
    begin
      found := True;
      break;
    end;

  if not found then
    Subscriptions.Add(aTopic + '=' + x.ToString);
  // Subscriptions.AddObject (aTopic, TObject (x));
//  StopPing;
  Parser.SendSubscribe(anID, aTopic, aQos);
//  StartPing;
end;

procedure TMyMQTTClient.DoSend(Sender: TObject; anID: Word; aRetry: integer;
  aStream: TMemoryStream);
var
  x: byte;
begin
  if FIsConnected then
  begin
//    aStream.Seek(0, soFromBeginning);
    aStream.Position := 0;
    aStream.Read(x, 1);
    if (TMQTTQOSType((x and $06) shr 1) in [qtAT_LEAST_ONCE, qtEXACTLY_ONCE])
      and (TMQTTMessageType((x and $F0) shr 4) in [ { mtPUBREL, } mtPUBLISH,
      mtSUBSCRIBE, mtUNSUBSCRIBE]) and (anID > 0) then
    begin
      InFlight.AddPacket(anID, aStream, aRetry, Parser.RetryTime);
      Mon('Message ' + IntToStr(anID) + ' created.');
    end;

//    aStream.Seek(0, soFromBeginning);
    aStream.Position := 0;
    SendData(aStream);
    Sleep(0);
  end;
end;

function TMyMQTTClient.GetClean: Boolean;
begin
  result := Parser.Clean;
end;

function TMyMQTTClient.GetClientID: UTF8String;
begin
  result := Parser.ClientID;
end;

function TMyMQTTClient.GetKeepAlive: Word;
begin
  result := Parser.KeepAlive;
end;

function TMyMQTTClient.GetMaxRetries: integer;
begin
  result := Parser.MaxRetries;
end;

function TMyMQTTClient.GetPassword: UTF8String;
begin
  result := Parser.Password;
end;

function TMyMQTTClient.GetRetryTime: cardinal;
begin
  result := Parser.RetryTime;
end;

function TMyMQTTClient.GetUsername: UTF8String;
begin
  result := Parser.Username;
end;

procedure TMyMQTTClient.RxSubAck(Sender: TObject; anID: Word;
  Qoss: array of TMQTTQOSType);
var
  j: integer;
  i, x: cardinal;
  function HiWord(L: cardinal): Word;
  begin
    result := L shr 16;
  end;

begin
  InFlight.DelPacket(anID);
  Mon('Message ' + IntToStr(anID) + ' disposed of.');
  for i := low(Qoss) to high(Qoss) do
  begin
    if i > 254 then
      break; // only valid for first 254
    for j := 0 to Subscriptions.Count - 1 do
    begin
      x := Subscriptions.ValueFromIndex[j].ToInteger;
      // x := cardinal (Subscriptions.Objects[j]);
      if (HiWord(x) = anID) and ((x and $0000FF00) shr 8 = i) then
        Subscriptions.ValueFromIndex[j] := IntToStr($FF00 + ord(Qoss[i]));
      // Subscriptions.Objects[j] :=  TObject ($ff00 + ord (Qoss[i]));
    end;
  end;
end;

procedure TMyMQTTClient.RxUnsubAck(Sender: TObject; anID: Word);
begin
  InFlight.DelPacket(anID);
  Mon('Message ' + IntToStr(anID) + ' disposed of.');
end;

// procedure TMyIndyMQTTClient.LinkConnected (Sender: TObject; ErrCode: Word);
// var
// aClientID : UTF8String;
//
// function TimeString : UTF8string;
// begin
/// /  86400  secs
// Result := UTF8String (IntToHex (Trunc (Date), 5) + IntToHex (Trunc (Frac (Time) * 864000), 7));
// end;
//
// begin
// if ErrCode = 0 then
// begin
// FGraceful := false;    // still haven't connected but expect to
// Parser.Reset;
// //   mon ('Time String : ' + Timestring);
// //=   mon ('xaddr ' + Link.GetXAddr);
// aClientID := ClientID;
// if aClientID = '' then
// aClientID := 'CID' + UTF8String (Link.Port.ToString); // + TimeString;
// if Assigned (FOnClientID) then
// FOnClientID (Self, aClientID);
// ClientID := aClientID;
// if Parser.Clean then
// begin
// InFlight.Clear;
// Releasables.Clear;
// end;
// if FBroker then
// Parser.SendBrokerConnect (aClientID, Parser.UserName, Parser.Password, KeepAlive, Parser.Clean)
// else
// Parser.SendConnect (aClientID, Parser.UserName, Parser.Password, KeepAlive, Parser.Clean);
// end;
// end;

procedure TMyMQTTClient.Mon(aStr: string);
begin
  if Assigned(FOnMon) then
    FOnMon(Self, aStr);
end;

procedure TMyMQTTClient.AfterConnected(Sender: TObject);
var
  aClientID: UTF8String;

  function TimeString: UTF8String;
  begin
    // 86400  secs
    result := UTF8String(IntToHex(Trunc(Date), 5) +
      IntToHex(Trunc(Frac(Time) * 864000), 7));
  end;

begin

  if FRecvThread = nil then
  begin
    FRecvThread := TMQTTReadThread.Create(Link);
    FRecvThread.FreeOnTerminate := True;
    FRecvThread.FProcessMessage := onData;
    FRecvThread.Start;
  end;

  FIsConnected := True;
  FIsClosed := False;

  begin
    FGraceful := false; // still haven't connected but expect to
    Parser.Reset;
    // mon ('Time String : ' + Timestring);
    // =   mon ('xaddr ' + Link.GetXAddr);
    aClientID := ClientID;
    if aClientID = '' then
      aClientID := 'CID' + UTF8String(Link.ToString); // + TimeString;
    if Assigned(FOnClientID) then
      FOnClientID(Self, aClientID);
    ClientID := aClientID;
    if Parser.Clean then
    begin
      InFlight.Clear;
      Releasables.Clear;
    end;
    if FBroker then
      Parser.SendBrokerConnect(aClientID, Parser.Username, Parser.Password,
        KeepAlive, Parser.Clean)
    else
      Parser.SendConnect(aClientID, Parser.Username, Parser.Password, KeepAlive,
        Parser.Clean);
  end;


  StartPing;

end;

procedure TMyMQTTClient.BeforeDisconnected(Sender: TObject);
begin
  Mon ('Link Closed...');
  if FPingTimer <> nil then
    FPingTimer.Terminate;

  if FRecvThread <> nil then
  begin
    FRecvThread.Terminate;
//    FRecvThread.WaitFor;
  end;
  if Assigned(FOnOffline) and (FOnline) then
    FOnOffline(Self, FGraceful);
  FOnline := false;
end;

procedure TMyMQTTClient.PingTimerProc(Sender: TObject);
begin
  Ping;
end;

procedure TMyMQTTClient.WillTimerProc(Sender: TObject);
var
  i: integer;
  bPacket: TMQTTPacket;
  WillClose: Boolean;
begin
  // send duplicates
  for i := InFlight.Count - 1 downto 0 do
  begin
    bPacket := InFlight.List[i];
    if bPacket.Counter > 0 then
    begin
      bPacket.Counter := bPacket.Counter - 1;
      if bPacket.Counter = 0 then
      begin
        bPacket.Retries := bPacket.Retries + 1;
        if bPacket.Retries <= MaxRetries then
        begin
          if bPacket.Publishing then
          begin
            InFlight.List.Remove(bPacket);
            Mon('Message ' + IntToStr(bPacket.ID) + ' disposed of..');
            Mon('Re-issuing Message ' + IntToStr(bPacket.ID) + ' Retry ' +
              IntToStr(bPacket.Retries));
            SetDup(bPacket.Msg, True);
            DoSend(Parser, bPacket.ID, bPacket.Retries, bPacket.Msg);
            bPacket.Free;
          end
          else
          begin
            Mon('Re-issuing PUBREL Message ' + IntToStr(bPacket.ID) + ' Retry '
              + IntToStr(bPacket.Retries));
            Parser.SendPubRel(bPacket.ID, True);
            bPacket.Counter := Parser.RetryTime;
          end;
        end
        else
        begin
          WillClose := True;
          if Assigned(FOnFailure) then
            FOnFailure(Self, frMAXRETRIES, WillClose);
          // if WillClose then Link.CloseDelayed;
          // if WillClose then Link.Disconnect;
        end;
      end;
    end;
  end;
end;

function TMyMQTTClient.NextMessageID: Word;
var
  i: integer;
  Unused: Boolean;
  aMsg: TMQTTPacket;
begin
  repeat
    Unused := True;
    FMessageID := FMessageID + 1;
    if FMessageID = 0 then
      FMessageID := 1; // exclude 0
    for i := 0 to InFlight.Count - 1 do
    begin
      aMsg := InFlight.List.Items[i];
      if aMsg.ID = FMessageID then
      begin
        Unused := false;
        break;
      end;
    end;
  until Unused;
  result := FMessageID;
end;

procedure TMyMQTTClient.onData(data: TStream);
begin
  Parser.Parse (data);
end;

procedure TMyMQTTClient.Ping;
begin
  if FIsConnected then
    Parser.SendPing;
//  try
//    if not FIsConnected then
//    begin
//      mon('Not Connected, will reconnnect...');
//      Connect;
//    end;
//    Parser.SendPing;
//  except
//    FIsConnected := False;
//    mon('Ping Error...');
//  end;
end;

procedure TMyMQTTClient.Publish(aTopic: UTF8String; aMessage: string;
aQos: TMQTTQOSType; aRetain: Boolean);
var
  i: integer;
  found: Boolean;
begin
  if FLocalBounce and Assigned(FOnMsg) then
  begin
    found := false;
    for i := 0 to Subscriptions.Count - 1 do
      // if IsSubscribed (UTF8String (Subscriptions[i]), aTopic) then
      if IsSubscribed(Subscriptions.Names[i], aTopic) then
      begin
        found := True;
        break;
      end;
    if found then
    begin
      Parser.RxQos := aQos;
      FOnMsg(Self, aTopic, aMessage, aQos, false);
    end;
  end;
//  StopPing;
  Parser.SendPublish(NextMessageID, aTopic, aMessage, aQos, false, aRetain);
//  StartPing;
end;

// procedure TMyIndyMQTTClient.TimerProc (var aMsg: TMessage);
// var
// i : integer;
// bPacket : TMQTTPacket;
// WillClose : Boolean;
// begin
// if aMsg.Msg = WM_TIMER then
// begin
// KillTimer (Timers, aMsg.WParam);
// case aMsg.WParam of
// 1 : begin
// Mon ('Connecting to ' + Host + ' on Port ' + IntToStr (Port));
// Link.Host := Host;
// Link.Port := Port;
// if not Link.Connected then
// begin
// try
// Link.Connect;
// except
// end;
// end;
// end;
// 2 : Ping;
// 3 : begin         // send duplicates
// for i := InFlight.Count - 1 downto 0 do
// begin
// bPacket := InFlight.List[i];
// if bPacket.Counter > 0 then
// begin
// bPacket.Counter := bPacket.Counter - 1;
// if bPacket.Counter = 0 then
// begin
// bPacket.Retries := bPacket.Retries + 1;
// if bPacket.Retries <=  MaxRetries then
// begin
// if bPacket.Publishing then
// begin
// InFlight.List.Remove (bPacket);
// mon ('Message ' + IntToStr (bPacket.ID) + ' disposed of..');
// mon ('Re-issuing Message ' + inttostr (bPacket.ID) + ' Retry ' + inttostr (bPacket.Retries));
// SetDup (bPacket.Msg, true);
// DoSend (Parser, bPacket.ID, bPacket.Retries, bPacket.Msg);
// bPacket.Free;
// end
// else
// begin
// mon ('Re-issuing PUBREL Message ' + inttostr (bPacket.ID) + ' Retry ' + inttostr (bPacket.Retries));
// Parser.SendPubRel (bPacket.ID, true);
// bPacket.Counter := Parser.RetryTime;
// end;
// end
// else
// begin
// WillClose := true;
// if Assigned (FOnFailure) then FOnFailure (Self, frMAXRETRIES, WillClose);
/// /                              if WillClose then Link.CloseDelayed;
// if WillClose then Link.Disconnect;
// end;
// end;
// end;
// end;
// SetTimer (Timers, 3, 100, nil);
// end;
// end;
// end;
// end;

procedure TMyMQTTClient.Unsubscribe(Topics: TStringList);
var
  i, j: integer;
begin
  if Topics = nil then
    exit;
  for i := 0 to Topics.Count - 1 do
  begin
    for j := Subscriptions.Count - 1 downto 0 do
      // if Subscriptions[j] = Topics[i] then
      if Subscriptions.Names[j] = Topics[i] then
      begin
        Subscriptions.Delete(j);
        break;
      end;
  end;
//  StopPing;
  Parser.SendUnsubscribe(NextMessageID, Topics);
//  StartPing;
end;

procedure TMyMQTTClient.Unsubscribe(aTopic: string);
var
  i: integer;
begin
  if aTopic = '' then
    exit;
  for i := Subscriptions.Count - 1 downto 0 do
    // if Subscriptions[i] = string (aTopic) then
    if Subscriptions.Names[i] = aTopic then
    begin
      Subscriptions.Delete(i);
      break;
    end;
//  StopPing;
  Parser.SendUnsubscribe(NextMessageID, aTopic);
//  StartPing;
end;

// procedure TMyIndyMQTTClient.LinkClosed (Sender: TObject; ErrCode: Word);
// begin
/// /  Mon ('Link Closed...');
/// /  KillTimer (Timers, 2);
/// /  KillTimer (Timers, 3);
// KillMyTimer(2);
// KillMyTimer(3);
// if Assigned (FOnOffline) and (FOnline) then
// FOnOffline (Self, FGraceful);
// FOnline := false;
// if FEnable then SetMyTimer (1, 6000); //SetTimer (Timers, 1, 6000, nil);
// end;

{ TMQTTPacketStore }

function TMQTTPacketStore.AddPacket(anID: Word; aMsg: TMemoryStream;
aRetry: cardinal; aCount: cardinal): TMQTTPacket;
begin
  result := TMQTTPacket.Create;
  result.ID := anID;
  result.Counter := aCount;
  result.Retries := aRetry;
//  aMsg.Seek(0, soFromBeginning);
  aMsg.Position := 0;
  result.Msg.CopyFrom(aMsg, aMsg.Size);
  List.Add(result);
end;

procedure TMQTTPacketStore.Assign(From: TMQTTPacketStore);
var
  i: integer;
  aPacket, bPacket: TMQTTPacket;
begin
  Clear;
  for i := 0 to From.Count - 1 do
  begin
    aPacket := From[i];
    bPacket := TMQTTPacket.Create;
    bPacket.Assign(aPacket);
    List.Add(bPacket);
  end;
end;

procedure TMQTTPacketStore.Clear;
var
  i: integer;
begin
//  for i := 0 to List.Count - 1 do
//    TMQTTPacket(List[i]).Free;
  List.Clear;
end;

function TMQTTPacketStore.Count: integer;
begin
  result := List.Count;
end;

constructor TMQTTPacketStore.Create;
begin
  Stamp := Now;
  List := TObjectList<TMQTTPacket>.Create;
end;

procedure TMQTTPacketStore.DelPacket(anID: Word);
var
  i: integer;
  aPacket: TMQTTPacket;
begin
  for i := List.Count - 1 downto 0 do
  begin
    aPacket := List[i];
    if aPacket.ID = anID then
    begin
      List.Remove(aPacket);
      // aPacket.Free;
      aPacket := nil;
      exit;
    end;
  end;
end;

destructor TMQTTPacketStore.Destroy;
begin
  Clear;
  List.Free;
  inherited;
end;

function TMQTTPacketStore.GetItem(Index: integer): TMQTTPacket;
begin
  if (Index >= 0) and (Index < Count) then
    result := List[Index]
  else
    result := nil;
end;

function TMQTTPacketStore.GetPacket(anID: Word): TMQTTPacket;
var
  i: integer;
begin
  for i := 0 to List.Count - 1 do
  begin
    result := List[i];
    if result.ID = anID then
      exit;
  end;
  result := nil;
end;

procedure TMQTTPacketStore.Remove(aPacket: TMQTTPacket);
begin
  List.Remove(aPacket);
end;

procedure TMQTTPacketStore.SetItem(Index: integer; const Value: TMQTTPacket);
begin
  if (Index >= 0) and (Index < Count) then
    List[Index] := Value;
end;

{ TMQTTPacket }

procedure TMQTTPacket.Assign(From: TMQTTPacket);
begin
  ID := From.ID;
  Stamp := From.Stamp;
  Counter := From.Counter;
  Retries := From.Retries;
  Msg.Clear;
//  From.Msg.Seek(0, soFromBeginning);
  From.Msg.Position := 0;
  Msg.CopyFrom(From.Msg, From.Msg.Size);
  Publishing := From.Publishing;
end;

constructor TMQTTPacket.Create;
begin
  ID := 0;
  Stamp := Now;
  Publishing := True;
  Counter := 0;
  Retries := 0;
  Msg := TMemoryStream.Create;
end;

destructor TMQTTPacket.Destroy;
begin
  if Msg <> nil then
    FreeAndNil(Msg);
  inherited;
end;

{ TMQTTMessage }

procedure TMQTTMessage.Assign(From: TMQTTMessage);
begin
  ID := From.ID;
  Stamp := From.Stamp;
  LastUsed := From.LastUsed;
  Retained := From.Retained;
  Counter := From.Counter;
  Retries := From.Retries;
  Topic := From.Topic;
  Message := From.Message;
  Qos := From.Qos;
end;

constructor TMQTTMessage.Create;
begin
  ID := 0;
  Stamp := Now;
  LastUsed := Stamp;
  Retained := false;
  Counter := 0;
  Retries := 0;
  Qos := qtAT_MOST_ONCE;
  Topic := '';
  Message := '';
end;

destructor TMQTTMessage.Destroy;
begin
  inherited;
end;

{ TMQTTMessageStore }

function TMQTTMessageStore.AddMsg(anID: Word; aTopic: UTF8String;
aMessage: UTF8String; aQos: TMQTTQOSType; aRetry, aCount: cardinal;
aRetained: Boolean): TMQTTMessage;
begin
  result := TMQTTMessage.Create;
  result.ID := anID;
  result.Topic := aTopic;
  result.Message := aMessage;
  result.Qos := aQos;
  result.Counter := aCount;
  result.Retries := aRetry;
  result.Retained := aRetained;
  List.Add(result);
end;

procedure TMQTTMessageStore.Assign(From: TMQTTMessageStore);
var
  i: integer;
  aMsg, bMsg: TMQTTMessage;
begin
  Clear;
  for i := 0 to From.Count - 1 do
  begin
    aMsg := From[i];
    bMsg := TMQTTMessage.Create;
    bMsg.Assign(aMsg);
    List.Add(bMsg);
  end;
end;

procedure TMQTTMessageStore.Clear;
var
  i: integer;
begin
  for i := 0 to List.Count - 1 do
    TMQTTMessage(List[i]).Free;
  List.Clear;
end;

function TMQTTMessageStore.Count: integer;
begin
  result := List.Count;
end;

constructor TMQTTMessageStore.Create;
begin
  Stamp := Now;
  List := TObjectList<TMQTTMessage>.Create();
end;

procedure TMQTTMessageStore.DelMsg(anID: Word);
var
  i: integer;
  aMsg: TMQTTMessage;
begin
  for i := List.Count - 1 downto 0 do
  begin
    aMsg := List[i];
    if aMsg.ID = anID then
    begin
      List.Remove(aMsg);
      aMsg.Free;
      exit;
    end;
  end;
end;

destructor TMQTTMessageStore.Destroy;
begin
  Clear;
  List.Free;
  inherited;
end;

function TMQTTMessageStore.GetItem(Index: integer): TMQTTMessage;
begin
  if (Index >= 0) and (Index < Count) then
    result := List[Index]
  else
    result := nil;
end;

function TMQTTMessageStore.GetMsg(anID: Word): TMQTTMessage;
var
  i: integer;
begin
  for i := 0 to List.Count - 1 do
  begin
    result := List[i];
    if result.ID = anID then
      exit;
  end;
  result := nil;
end;

procedure TMQTTMessageStore.Remove(aMsg: TMQTTMessage);
begin
  List.Remove(aMsg);
end;

procedure TMQTTMessageStore.SetItem(Index: integer; const Value: TMQTTMessage);
begin
  if (Index >= 0) and (Index < Count) then
    List[Index] := Value;
end;

{ TMQTTSession }

constructor TMQTTSession.Create;
begin
  ClientID := '';
  Stamp := Now;
  InFlight := TMQTTPacketStore.Create;
  Releasables := TMQTTMessageStore.Create;
end;

destructor TMQTTSession.Destroy;
begin
  InFlight.Clear;
  InFlight.Free;
  Releasables.Clear;
  Releasables.Free;
  inherited;
end;

{ TMQTTSessionStore }

procedure TMQTTSessionStore.Clear;
var
  i: integer;
begin
  for i := 0 to List.Count - 1 do
    TMQTTSession(List[i]).Free;
  List.Clear;
end;

function TMQTTSessionStore.Count: integer;
begin
  result := List.Count;
end;

constructor TMQTTSessionStore.Create;
begin
  Stamp := Now;
  List := TList.Create;
end;

procedure TMQTTSessionStore.DeleteSession(ClientID: UTF8String);
var
  aSession: TMQTTSession;
begin
  aSession := GetSession(ClientID);
  if aSession <> nil then
  begin
    List.Remove(aSession);
    aSession.Free;
  end;
end;

destructor TMQTTSessionStore.Destroy;
begin
  Clear;
  List.Free;
  inherited;
end;

function TMQTTSessionStore.GetItem(Index: integer): TMQTTSession;
begin
  if (Index >= 0) and (Index < Count) then
    result := List[Index]
  else
    result := nil;
end;

function TMQTTSessionStore.GetSession(ClientID: UTF8String): TMQTTSession;
var
  i: integer;
begin
  for i := 0 to List.Count - 1 do
  begin
    result := List[i];
    if result.ClientID = ClientID then
      exit;
  end;
  result := nil;
end;

procedure TMQTTSessionStore.RestoreSession(ClientID: UTF8String;
aClient: TMyMQTTClient);
var
  aSession: TMQTTSession;
begin
  aClient.InFlight.Clear;
  aClient.Releasables.Clear;
  aSession := GetSession(ClientID);
  if aSession <> nil then
  begin
    aClient.InFlight.Assign(aSession.InFlight);
    aClient.Releasables.Assign(aSession.Releasables);
  end;
end;

procedure TMQTTSessionStore.StoreSession(ClientID: UTF8String;
aClient: TMyMQTTClient);
var
  aSession: TMQTTSession;
begin
  aSession := GetSession(ClientID);
  if aSession <> nil then
  begin
    aSession := TMQTTSession.Create;
    aSession.ClientID := ClientID;
    List.Add(aSession);
  end;

  aSession.InFlight.Assign(aClient.InFlight);
  aSession.Releasables.Assign(aClient.Releasables);
end;

procedure TMQTTSessionStore.SetItem(Index: integer; const Value: TMQTTSession);
begin
  if (Index >= 0) and (Index < Count) then
    List[Index] := Value;
end;

end.
