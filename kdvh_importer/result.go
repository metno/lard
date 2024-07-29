package main

type Result[T any] struct {
	Ok  T
	Err error
}

func Ok[T any](ok T) Result[T] {
	return Result[T]{ok, nil}
}

func Err[T any](err error) Result[T] {
	var ok T
	return Result[T]{ok, err}
}
