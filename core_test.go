package mode_client

import (
	"crypto/tls"
	"testing"
)

func TestDeviceContext_SetPKCS12ClientCertificate(t *testing.T) {
	type fields struct {
		DeviceID           uint64
		AuthToken          string
		TLSClientAuth      bool
		PKCS12FileName     string
		PKCS12Password     string
		InsecureSkipVerify bool
	}
	tests := []struct {
		name                   string
		fields                 fields
		want                   *tls.Config
		shouldBeNil            bool
		wantInsecureSkipVerify bool
		wantErr                bool
	}{
		{
			name: "base case",
			fields: fields{
				DeviceID:       12345,
				TLSClientAuth:  true,
				PKCS12FileName: "fixtures/client1.p12",
				PKCS12Password: "pwd",
			},
			shouldBeNil:            false,
			wantInsecureSkipVerify: false,
			wantErr:                false,
		},
		{
			name: "InsecureSkipVerify: true",
			fields: fields{
				DeviceID:           12345,
				TLSClientAuth:      true,
				PKCS12FileName:     "fixtures/client1.p12",
				PKCS12Password:     "pwd",
				InsecureSkipVerify: true,
			},
			shouldBeNil:            false,
			wantInsecureSkipVerify: true,
			wantErr:                false,
		},
		{
			name: "password is wrong",
			fields: fields{
				DeviceID:       12345,
				TLSClientAuth:  true,
				PKCS12FileName: "fixtures/client1.p12",
				PKCS12Password: "xxxxxx",
			},
			shouldBeNil: true,
			wantErr:     true,
		},
		{
			name: "PKCS#12 file doesn't exist",
			fields: fields{
				DeviceID:       12345,
				TLSClientAuth:  true,
				PKCS12FileName: "fixtures/xxxxx.p12",
				PKCS12Password: "pwd",
			},
			shouldBeNil: true,
			wantErr:     true,
		},
		{
			name: "PKCS#12 file is invalid",
			fields: fields{
				DeviceID:       12345,
				TLSClientAuth:  true,
				PKCS12FileName: "fixtures/invalid.p12",
				PKCS12Password: "pwd",
			},
			shouldBeNil: true,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dc := &DeviceContext{
				DeviceID:      tt.fields.DeviceID,
				AuthToken:     tt.fields.AuthToken,
				TLSClientAuth: tt.fields.TLSClientAuth,
			}
			err := dc.SetPKCS12ClientCertificate(tt.fields.PKCS12FileName, tt.fields.PKCS12Password, tt.fields.InsecureSkipVerify)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeviceContext.buildConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if dc.TLSConfig != nil {
				if tt.shouldBeNil && dc.TLSConfig.Certificates != nil {
					t.Errorf("DeviceContext.buildConfig() = %v, but should be nil", dc.TLSConfig)
					return
				}
				if dc.TLSConfig != nil && dc.TLSConfig.InsecureSkipVerify != tt.wantInsecureSkipVerify {
					t.Errorf("DeviceContext.buildConfig() InsecureSkipVerify = %v, wantInsecureSkipVerify %v", dc.TLSConfig.InsecureSkipVerify, tt.wantInsecureSkipVerify)
					return
				}
			} else {
				if !tt.shouldBeNil {
					t.Errorf("DeviceContext.buildConfig() is nil, but should be not nil")
					return
				}
			}
		})
	}
}
