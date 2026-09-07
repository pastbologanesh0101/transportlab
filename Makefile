# TransportLab -- convenience targets. Pure stdlib Python 3.9+, nothing to install.

PY ?= python3

.PHONY: run arena mux sweep headless test clean

run:            ## dashboard at http://127.0.0.1:8080
	$(PY) run.py

arena:          ## dashboard: Reno vs CUBIC vs BBR over one bottleneck
	$(PY) run.py --flows 3 --flow-cc reno,cubic,bbr --preset transoceanic --size 2

mux:            ## dashboard: 4 QUIC-style streams on a burst-loss link
	$(PY) run.py --mux 4 --preset mobile_handoff --size 2

sweep:          ## head-less loss sweep vs the Mathis model
	$(PY) run.py --sweep --cc reno --preset wifi_cafe --size 0.3

headless:       ## one head-less transfer, prints JSON
	$(PY) run.py --auto --preset wifi_cafe --cc reno --size 1

test:           ## wire format + loopback + arena tests
	$(PY) -m unittest discover -s tests -v

clean:
	rm -f sample/*.bin sample/*.png sample/*.jpg
	find . -name __pycache__ -type d -exec rm -rf {} +
