
## venv setup

It's usually a good idea to install Python dependencies in a virtual environment to avoid
conflicts between projects.

To setup a venv with the latest release version of confluent-kafka and dependencies of all examples installed:

```
$ python3 -m venv venv_examples
$ source venv_examples/bin/activate
$ cd examples
$ pip install -r requirements.txt
```

To setup a venv that uses the current source tree version of confluent_kafka, you
need to have a C compiler and librdkafka installed
([from a package](https://github.com/edenhill/librdkafka#installing-prebuilt-packages), or
[from source](https://github.com/edenhill/librdkafka#build-from-source)). Then:

```
$ python2 -m venv ~/git-repos/spark-structured-streaming/venv
$ source ~/git-repos/spark-structured-streaming/venv/bin/activate
$ python setup.py develop
$ cd examples
$ pip install -r requirements.pip
```

When you're finished with the venv:

```
$ deactivate
```