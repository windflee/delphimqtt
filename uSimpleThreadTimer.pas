unit uSimpleThreadTimer;

interface

uses
  System.Classes,
  System.SyncObjs;

type
  TSimpleThreadTimer = class(TThread)
  private
    { Private declarations }
    FOnTimer: TNotifyEvent;
    FEvent: TEvent;
    FInterval: Cardinal;
    procedure SetOnTimer(const Value: TNotifyEvent);
  protected
    procedure Execute; override;
    procedure TerminatedSet; override;
  public
    constructor Create;
    destructor Destroy; override;

    property Interval: Cardinal read FInterval write FInterval;
    property OnTimer: TNotifyEvent write SetOnTimer;
  end;

implementation

{ 
  Important: Methods and properties of objects in visual components can only be
  used in a method called using Synchronize, for example,

      Synchronize(UpdateCaption);  

  and UpdateCaption could look like,

    procedure TSimpleThreadTimer.UpdateCaption;
    begin
      Form1.Caption := 'Updated in a thread';
    end; 
    
    or 
    
    Synchronize( 
      procedure 
      begin
        Form1.Caption := 'Updated in thread via an anonymous method' 
      end
      )
    );
    
  where an anonymous method is passed.
  
  Similarly, the developer can call the Queue method with similar parameters as 
  above, instead passing another TThread class as the first parameter, putting
  the calling thread in a queue with the other thread.
    
}

{ TSimpleThreadTimer }

constructor TSimpleThreadTimer.Create;
begin
  inherited Create(True);
  FEvent := TEvent.Create(nil, False, False, '');
  FInterval := 10000;
end;

destructor TSimpleThreadTimer.Destroy;
begin
  FEvent.Free;
  inherited;
end;

procedure TSimpleThreadTimer.Execute;
begin
  { Place thread code here }
  while not Terminated do
  begin
    FEvent.WaitFor(FInterval);

    if not Terminated then
    begin
      if Assigned(FOnTimer) then
        FOnTimer(Self);
    end;
  end;
end;

procedure TSimpleThreadTimer.SetOnTimer(const Value: TNotifyEvent);
begin
  FOnTimer := Value;
end;

procedure TSimpleThreadTimer.TerminatedSet;
begin
  inherited;
  FEvent.SetEvent;
end;

end.
