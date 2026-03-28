@echo off

set VERSION=%1

echo Building docker image version %VERSION%

docker build -t nguyenphung_telecom_cdr_auto_loader:%VERSION% -f docker\Dockerfile .

echo Saving image as tar.gz

docker save nguyenphung_telecom_cdr_auto_loader:%VERSION% -o docker\docker_images\nguyenphung_telecom_cdr_auto_loader_%VERSION%.tar.gz
