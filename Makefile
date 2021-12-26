PYTHON_FILES := $(shell find ./ -type f -name '*.py' | grep -v __main__)


.PHONY: doctest
doctest: $(PYTHON_FILES)
	python3 -m doctest $^
	@echo "# doctest passed"