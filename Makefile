default: tests

.PHONY: manual tests cleanup upload sdist wheel install

PYTHON=python3
PIP=pip3

# Install using pip
install:
	${PIP} install --upgrade --user --editable .

# Source distribution
sdist:
	${PYTHON} setup.py sdist

# Pure Python Wheel
wheel:
	${PYTHON} setup.py bdist_wheel

tests:
	pytest

# Remove distribution files
cleanup:
	rm -rf dist/ build/ *.egg-info/

upload: tests cleanup sdist wheel
	cd dist; \
  	for file in *; do \
		  twine check $$file && \
		  twine upload $$file; \
		done
