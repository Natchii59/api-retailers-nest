#!/bin/bash

# Créer le répertoire de sortie s'il n'existe pas
mkdir -p go

# Générer les fichiers Go
protoc -I . \
  --go_out=go --go_opt=paths=source_relative \
  --go-grpc_out=go --go-grpc_opt=paths=source_relative \
  *.proto

echo "✅ Fichiers proto Go générés dans packages/proto/go/"

# TypeScript (NestJS)
# mkdir -p ts
# protoc -I . \
#   --plugin=../../node_modules/.bin/protoc-gen-ts_proto \
#   --ts_proto_out=ts \
#   --ts_proto_opt=nestJs=true,outputServices=grpc-js \
#   *.proto

# echo "✅ Fichiers proto TypeScript générés dans packages/proto/ts/"