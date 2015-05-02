# ParS3

ParS3 is a programming for downloading and uploading files from Amazon's
S3 service, in parallel.

The project was created due to the fact that (at the time) s3cmd would
overflow when getting millions of files. ParS3 uses channels to
asynchronously download files before the entirety of the directory is
listed, which means it's much quicker than some other implementations.

Where it really shines is pulling down log files from Elastic Map
Reduce. It's really good at that.

## Getting Started

### Installation
- Install Golang: https://golang.org/doc/install
- Get ParS3: go get github.com/nickrobison/ParS3

### Usage

You need to create a json file in your home user directory named ParS3
with the following contents:

```javascript
{
  "AWSCredentials": {
    "AccessKey": (your AWS Access Key),
    "SecretKey": (your AWS Secret Key)
  }
}
```

ParS3 currently supports the ability to get/put files using the
following syntax:

```bash
ParS3 get s3://(source-bucket)/(source-directory) (destination-directory)/

ParS3 put (source-directory)/*
s3://(destination-bucket)/(destination-diretory)
```

More options will be coming in the near future.

## Other Notes

I'm just now getting around to posting this project but it stands as the first go program that I ever wrote (First commit was July,
2013) so yes, it is terrible. I knew nothing of interfaces or really
any idiomatic go. So the source code is terrible, but the program works
and I'm working on a pretty substantial refactoring that should make
things much better.

I'm also planning on extending it to support more storage platforms, so
hopefully it'll become even more useful. 

### TODO List:
- [ ] Refactor into interfaces
- [ ] Add OpenStack Swift support
- [ ] Add Azure Storage
- [ ] Add HDFS support
- [ ] Remove all the cruft from a more ignorant self
- [ ] Fix directory listing

Good luck to us all.
