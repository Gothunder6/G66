[Setup]
AppId={{A0D39917-1F4E-4A85-9B7B-2FD8F5B905A8}
AppName=BTP Forex Signal
AppVersion=1.0
AppPublisher=BEGIN TO PRO
DefaultDirName={pf}\BTP Forex Signal
DefaultGroupName=BTP Forex Signal
OutputBaseFilename=BTP_Forex_Signal_Installer_v1.0
OutputDir=Output
Compression=lzma
SolidCompression=yes

[Files]
Source: "dist\btp_signal.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\BTP Forex Signal"; Filename: "{app}\btp_signal.exe"
