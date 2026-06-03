#!/bin/bash

curl -L -o ./tcg-market-intelligence.zip\
  https://www.kaggle.com/api/v1/datasets/download/sailorpepe/tcg-market-intelligence

unzip tcg-market-intelligence.zip

rm tcg-market-intelligence.zip
