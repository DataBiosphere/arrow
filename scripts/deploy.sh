#!/usr/bin/env bash
set -eo pipefail

gcloud app deploy --project=terra-arrow-$ENV
