docker build -t minio-with-chargy-data:latest .
docker tag minio-with-chargy-data:latest docker.gijoe88.com/minio-with-chargy-data:latest
docker tag minio-with-chargy-data:latest gijoe88/minio-with-chargy-data:latest
docker push docker.gijoe88.com/minio-with-chargy-data:latest
docker push gijoe88/minio-with-chargy-data:latest
