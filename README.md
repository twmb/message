message
-------

Check the [godoc](http://godoc.org/github.com/twmb/channel).

Package message is useful for when you want to continue reading
after receiving an "end of messages" error. Sometimes, you just
haven't finished reading. More data could be published. You've
only reached the end of the *current* messages.
