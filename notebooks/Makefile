notebooks := $(wildcard *.ipynb)

.PHONY: test $(notebooks)

test: $(notebooks)

$(notebooks):
	papermill $@ /dev/null
