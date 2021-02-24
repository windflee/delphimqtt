unit uMyMQTTReadThread;

interface

uses
  Classes, SysUtils, uMyMQTT, blcksock;

type

//   TProcessMessage = procedure(dataStream: TBytes) of object;
  TProcessMessage = procedure(dataStream: TStream) of object;

  TMQTTReadThread = class(TThread)
  private
    { Private declarations }
    FPSocket: TTCPBlockSocket;
    ms: TMemoryStream;
  protected
    procedure Execute; override;
  public
    FProcessMessage: TProcessMessage;

    constructor Create(var Socket: TTCPBlockSocket);
    destructor Destroy; override;
  end;

implementation

{ TMQTTReadThread }

constructor TMQTTReadThread.Create(var Socket: TTCPBlockSocket);
begin
  inherited Create(True);
  ms := TMemoryStream.Create;
  FPSocket := Socket;
end;

destructor TMQTTReadThread.Destroy;
begin
  if ms <> nil then
  begin
    FreeAndNil(ms);
  end;
  inherited;
end;

procedure TMQTTReadThread.Execute;
var buffer: TBytes;
    i: integer;
    AByte: Byte;

begin
  setlength(buffer, c64k);
  while not Terminated do
  begin
    try
      if (FPSocket <> nil)  then
      begin

        begin

          i := FPSocket.RecvBufferEx(@buffer[0], c64k, 1000);
          if i > 0 then
          begin
//            if not Terminated then
//            begin
//                try
//                  if Assigned(FProcessMessage) then
//                  begin
////                    FProcessMessage(buffer);
////                    Queue(procedure begin FProcessMessage(buffer) end);
//                    Synchronize(
//                      procedure
//                      begin
//
//                        FProcessMessage(buffer);
//
//                      end);
//                  end;
//                except
//                end;
//            end;

            if ms <> nil then
            begin
              ms.Clear;
              ms.Write(buffer, i);
              if not Terminated then
              begin
                try
                  if Assigned(FProcessMessage) then
                  begin
//                    FProcessMessage(ms)
                    Synchronize(
                      procedure
                      begin
                        FProcessMessage(ms)
                      end);
                  end;
                except
                end;
              end;
            end;

          end;

        end;

      end;
    except

    end;
  end;
end;

end.
