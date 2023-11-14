import os
import uuid
import datetime
from urllib3.exceptions import MaxRetryError
from minio import Minio
from minio.error import S3Error
from fastapi import APIRouter, File, UploadFile, Depends, status
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse, Response

router = APIRouter(prefix="/file", tags=['File'])


# get_bucket_name 根据当前是获取桶名
def get_bucket_name() -> str:
    now = datetime.datetime.now()
    return "{}{}{}".format(now.year, now.month, now.day)


# get_client 获取 Minio 客户端连接对象
def get_client():
    try:
        client = Minio(
            "127.0.0.1:9000",
            access_key="DlkbXZW6UyFLYLSbOCQJ",
            secret_key="MBjYEv7iTYDqfJlfi29JJUbx8KKKafxEm2aLg7rp",
            secure=False
        )
        if not client.bucket_exists(bucket_name=get_bucket_name()):
            client.make_bucket(get_bucket_name())
        return client
    except S3Error as exc:
        print("error occurred", exc)
        return None
    except MaxRetryError as exc:
        print("error occurred", exc)
        return None


@router.post("/")
async def upload_file(file: UploadFile, clien: Minio = Depends(get_client)):
    result = dict()
    if clien is None:
        result.update(
            {"message": "内部错误", "code": status.HTTP_500_INTERNAL_SERVER_ERROR})
    else:
        filename = "{}.{}".format(uuid.uuid4(), file.filename.split(".")[-1])
        response = clien.put_object(
            get_bucket_name(), filename, file.file, file.size)
        result.update({"message": "执行成功", "code": status.HTTP_201_CREATED, "data": {
            "uri": "{}/{}".format(response.bucket_name, response.object_name),
        }})
    return JSONResponse(content=result, status_code=result.get("code"))


@router.get("/{bucket_name}/{file_name}")
async def get_file(bucket_name: str, file_name: str, clien: Minio = Depends(get_client)):
    # Get data of an object.
    try:
        temp_path = f"temp/{bucket_name}/{file_name}"
        clien.fget_object(
            bucket_name=bucket_name, object_name=file_name, file_path=temp_path)
        return FileResponse(temp_path)
    except S3Error as exc:
        print("error occurred", exc)
        return Response(status_code=status.HTTP_404_NOT_FOUND)


@router.delete("/{bucket_name}/{file_name}")
async def remove(bucket_name: str, file_name: str, clien: Minio = Depends(get_client)):
    clien.remove_object(bucket_name, file_name)
    return JSONResponse(content={"code": status.HTTP_200_OK, "message": "操作成功"})
