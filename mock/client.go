// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/conduitio-labs/conduit-connector-databricks (interfaces: Client)

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	databricks "github.com/conduitio-labs/conduit-connector-databricks"
	gomock "github.com/golang/mock/gomock"
)

// Client is a mock of Client interface.
type Client struct {
	ctrl     *gomock.Controller
	recorder *ClientMockRecorder
}

// ClientMockRecorder is the mock recorder for Client.
type ClientMockRecorder struct {
	mock *Client
}

// NewClient creates a new mock instance.
func NewClient(ctrl *gomock.Controller) *Client {
	mock := &Client{ctrl: ctrl}
	mock.recorder = &ClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *Client) EXPECT() *ClientMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *Client) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *ClientMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*Client)(nil).Close))
}

// Open mocks base method.
func (m *Client) Open(arg0 context.Context, arg1 databricks.Config) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Open", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Open indicates an expected call of Open.
func (mr *ClientMockRecorder) Open(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Open", reflect.TypeOf((*Client)(nil).Open), arg0, arg1)
}