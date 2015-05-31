#!/bin/sh

YAK_URL=yak://127.0.0.1:7700/tests
env YAK_HEAD=$YAK_URL YAK_TAIL=$YAK_URL cargo test -v
