# aws-lambda-gw2pvo-uploader
This is AWS Lambda-packed Goodwe (SEMS) to PVO uploader. 
It was initially inspired by https://github.com/markruys/gw2pvo and converted from my fork of it, with extensions for HomeKit.


## Python 3.13

The current Lambda runtime target is Python 3.13. GoodWe access now uses the SEMS+ cross-login flow and the regional V3 monitor endpoint while keeping the existing `GoodWeApi(...).getCurrentReadings()` interface used by the Lambda.

The release layer is rebuilt from source with `requirements-layer.txt`; the old committed layer ZIP is legacy only. Dark Sky support was removed from the current Lambda path.

Credentials remain runtime inputs from the Lambda event and are not stored in this repository.
