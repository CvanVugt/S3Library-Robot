import boto3
import botocore
import os
import fnmatch

class Keywords(object):

    def Set_S3_Credentials(self, access_key, secret_key):
        """
        Initializes a connection to an S3 API endpoint

        Must be called before any other keywords are used

        _access_key_ is the AWS access key

        _secret_key_ is the AWS secret key
        """
        self._conn = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        self._s3=self._conn.resource('s3')

    def Upload_Object(self, path, name, bucket):
        """
        Uploads given file into bucket

        _path_: local path to object to upload.  if _path_ is a directory, then
        an exception will be raised

        _name_: the name you want to give the object in S3. (E.g.) One should name the object in accordance to the executed test, so that the object can be easily identified. Additionally, when one wants to place the object in a specific directory in a bucket, one should put the name of that directory before the objects name with a slash. (E.g.) evergas/object1 will place object1 in the evergas directory.

        _bucket_: Target bucket.

        """
        if os.path.isdir(path):
            raise NotImplementedError("Can't upload directories")

        s3bucket = self._s3.Bucket(bucket)
        s3bucket.upload_file(path, name)

    def Upload_Object_Name_Contains(self, dir, keyword, bucket, name):
        """
        Uploads given file into bucket

        _path_: local path to object to upload.  if _path_ is a directory, then
        an exception will be raised

        _name_: the name you want to give the object in S3. (E.g.) One should name the object in accordance to the executed test, so that the object can be easily identified. Additionally, when one wants to place the object in a specific directory in a bucket, one should put the name of that directory before the objects name with a slash. (E.g.) evergas/object1 will place object1 in the evergas directory.

        _bucket_: Target bucket.

        """
        s3bucket = self._s3.Bucket(bucket)
        isFound = False
        for fname in os.listdir(dir):
            if keyword in fname:
                s3bucket.upload_file(dir + '/' + fname, name)
                isFound = True

        if not isFound:
            raise NotImplementedError("Name not found in dir")

    def Delete_Object_In_Directory_Name_Contains(self, keyword, dir):
        """
        Uploads given file into bucket

        _path_: local path to object to upload.  if _path_ is a directory, then
        an exception will be raised

        _name_: the name you want to give the object in S3. (E.g.) One should name the object in accordance to the executed test, so that the object can be easily identified. Additionally, when one wants to place the object in a specific directory in a bucket, one should put the name of that directory before the objects name with a slash. (E.g.) evergas/object1 will place object1 in the evergas directory.

        _bucket_: Target bucket.

        """
        isFound = False
        for fname in os.listdir(dir):
            if keyword in fname:
                os.remove(dir + '/' + fname)
                isFound = True

        if not isFound:
            raise NotImplementedError("Name not found in dir")        

    def Download_Object(self, path, obj, bucket):
        """
        Downloads given file from a bucket to a specified directory

        _path_: local path to where you want the object to be downloaded to.  if _path_ is only a directory without the object name, then
        a windows error will be raised. Hence, specify a name for the object here as well. A path usually looks like C:/Desktoporsomething/Downloadedfiles/DownloadedObject

        _obj_: The name of the object in the S3 bucket. If you have folders in your bucket, include these as well (e.g. directory1/object1)

        _bucket_: Target bucket.

        """
        try:
            s3bucket = self._s3.Bucket(bucket)
            s3bucket.download_file(obj, path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    def Delete_Object(self, obj, bucket):
        """
        Deletes an object in a specific bucket

        _obj_: target object

        _bucket_: target bucket

        """
        obj = self._s3.Object(bucket, obj)
        obj.get()
        obj.delete()

    def Get_Object(self, obj, bucket):
        """
        Can be used to see if an object is present

        _obj_: target object

        _bucket_: target bucket

        """
        obj = self._s3.Object(bucket, obj)
        obj.get()

    def Create_Bucket(self, bucket):
        """
        Creates a bucket

        _bucket_: name of bucket to create
        """
        self._s3.create_bucket(Bucket=bucket)

    def Delete_Bucket(self, bucket):
        """
        Deletes a bucket

        _bucket_: name of bucket to delete
        """
        bucket = self._s3.Bucket(bucket)
        bucket.delete()

