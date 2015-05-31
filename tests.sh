#!/bin/sh

env YAK_HEAD=yak://127.0.0.1:7700/tests YAK_TAIL=yak://127.0.0.1:7710/tests cargo test -v
