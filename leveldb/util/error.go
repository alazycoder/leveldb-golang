package util

import (
	"errors"
	"fmt"
)

type ErrorNo uint32

const (
	ErrOk ErrorNo = iota
	ErrUnknown
	ErrWriteFileFailed
	ErrFlushFileFailed
	ErrSyncFileFailed
	ErrSeekFileFailed
	ErrCheckCrcFailed
	ErrReadFileFailed
	ErrUnknownRecordType
	ErrBadRecordLength
	ErrMissingStart
	ErrInMiddleRecord
	ErrPartialRecordWithoutEnd
)

type LevelDbError struct {
	errorNo ErrorNo
	msg     string
}

func NewLevelDbError(errNo ErrorNo, msg string, args ...any) *LevelDbError {
	return &LevelDbError{
		errorNo: errNo,
		msg:     fmt.Sprintf(msg, args...),
	}
}

func (err *LevelDbError) Error() string {
	return fmt.Sprintf("ErrorNo: %d, Msg: %s", err.errorNo, err.msg)
}

func GetErrorNo(err error) ErrorNo {
	if err == nil {
		return ErrOk
	}
	var levelDbErr *LevelDbError
	ok := errors.As(err, &levelDbErr)
	if !ok {
		return ErrUnknown
	}
	return levelDbErr.errorNo
}
