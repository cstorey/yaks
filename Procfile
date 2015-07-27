head: sleep 2; ./target/debug/yak_server $(mktemp -d /tmp/yaks/head-XXXXXXXX) 127.0.0.1:7700 127.0.0.1:7701
middle: sleep 1; ./target/debug/yak_server $(mktemp -d /tmp/yaks/middle-XXXXXXXX) 127.0.0.1:7701 127.0.0.1:7710
tail: sleep 0; ./target/debug/yak_server $(mktemp -d /tmp/yaks/tail-XXXXXXXX) 127.0.0.1:7710
