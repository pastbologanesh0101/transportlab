# TransportLab -- convenience targets. Pure stdlib Python 3.9+, nothing to install.

PY ?= python3

.PHONY: run demo headless test clean

run:            ## launch the dashboard (http://127.0.0.1:8080)
	$(PY) run.py

demo:           ## dashboard preloaded with the satellite scenario
	$(PY) run.py --preset satellite --arq selective_repeat --cc cubic --size 2

headless:       ## one head-less transfer, prints a JSON summary
	$(PY) run.py --auto --preset wifi_cafe --arq selective_repeat --cc reno --size 1

test:           ## wire-format + end-to-end loopback tests
	$(PY) -m unittest discover -s tests -v

clean:
	rm -f sample/source.bin sample/received.bin
	find . -name __pycache__ -type d -exec rm -rf {} +
