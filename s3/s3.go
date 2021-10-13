package s3

import (
	"bytes"
	"io"
	"net/http"
	"path"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sputnik-systems/backups-storage"
)

type S3 struct {
	c              *s3.S3
	bucket, prefix string
	partSize       int64
}

type FileInfo struct {
	name  string
	size  int64
	mtime time.Time
	isdir bool
}

func NewStorage(sess *session.Session, bucket, prefix string) storage.Storage {
	partSize := int64(100 * 1024 * 1024)

	return &S3{
		c:        s3.New(sess),
		bucket:   bucket,
		prefix:   prefix,
		partSize: partSize,
	}
}

func (s *S3) List() ([]storage.FileInfo, error) {
	return s.list(s.prefix)
}

func (s *S3) list(prefix string) ([]storage.FileInfo, error) {
	in := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}

	fi := make([]storage.FileInfo, 0)
	err := s.c.ListObjectsV2Pages(in, func(page *s3.ListObjectsV2Output, last bool) bool {
		for _, o := range page.Contents {
			fi = append(fi, &FileInfo{*o.Key, *o.Size, *o.LastModified, false})
		}

		return !last
	})
	if err != nil {
		return fi, err
	}

	// sort by mtime dicreasing
	sort.Slice(fi, func(i, j int) bool {
		return fi[i].ModTime().Unix() < fi[j].ModTime().Unix()
	})

	// calc directories
	di := make([]storage.FileInfo, 0)
	for _, o := range fi {
		name := path.Dir(o.Name()) + "/"
		di = append(di, &FileInfo{name, int64(0), o.ModTime(), true})
	}
	di = func(in []storage.FileInfo) []storage.FileInfo {
		names := make(map[string]struct{})
		out := make([]storage.FileInfo, 0)
		for _, d := range in {
			if _, ok := names[d.Name()]; !ok {
				names[d.Name()] = struct{}{}
				out = append(out, d)
			}
		}

		return out
	}(di)

	fi = append(fi, di...)

	sort.Slice(fi, func(i, j int) bool {
		return fi[i].Name() > fi[j].Name()
	})

	return fi, nil
}

func (s *S3) Delete(name string) error {
	prefix := path.Join(s.prefix, name)
	fi, err := s.list(prefix)
	if err != nil {
		return err
	}

	oi := make([]*s3.ObjectIdentifier, 0)
	for _, o := range fi {
		oi = append(oi, &s3.ObjectIdentifier{Key: aws.String(o.Name())})
	}

	in := &s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket),
		Delete: &s3.Delete{
			Objects: oi,
		},
	}

	if _, err := s.c.DeleteObjects(in); err != nil {
		return err
	}

	return nil
}

func (s *S3) Upload(name string, buf io.Reader) error {
	var mupload *s3.CreateMultipartUploadOutput
	var mparts []*s3.CompletedPart
	var part *s3.CompletedPart
	var err error

	key := path.Join(s.prefix, name)
	b := make([]byte, s.partSize)
	for {
		n, err := buf.Read(b)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		b = b[:n]

		if int64(n) == s.partSize {
			if mupload == nil {
				contentType := http.DetectContentType(b)

				in := &s3.CreateMultipartUploadInput{
					Bucket:      aws.String(s.bucket),
					Key:         aws.String(key),
					ContentType: aws.String(contentType),
				}

				mupload, err = s.c.CreateMultipartUpload(in)
				if err != nil {
					return err
				}

				mparts = make([]*s3.CompletedPart, 0)
			}

			part, err = s.uploadPart(key, mupload.UploadId, int64(len(mparts)+1), b)
			if err != nil {
				return err
			}

			mparts = append(mparts, part)
		}
	}

	if mupload == nil {
		in := &s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(b),
		}

		if _, err = s.c.PutObject(in); err != nil {
			return err
		}
	} else {
		part, err = s.uploadPart(key, mupload.UploadId, int64(len(mparts)+1), b)
		if err != nil {
			return err
		}

		mparts = append(mparts, part)

		in := &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(s.bucket),
			Key:      aws.String(key),
			UploadId: mupload.UploadId,
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: mparts,
			},
		}

		if _, err = s.c.CompleteMultipartUpload(in); err != nil {
			return err
		}
	}

	return nil
}

func (s *S3) Download(name string, buf io.Writer) error {
	key := path.Join(s.prefix, name)

	in := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}

	o, err := s.c.GetObject(in)
	if err != nil {
		return err
	}

	b := make([]byte, s.partSize)
	for {
		n, err := o.Body.Read(b)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		b = b[:n]

		n, err = buf.Write(b)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *S3) uploadPart(key string, uploadId *string, partNumber int64, body []byte) (*s3.CompletedPart, error) {
	contentLength := int64(len(body))

	pi := &s3.UploadPartInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		UploadId:      uploadId,
		Body:          bytes.NewReader(body),
		PartNumber:    aws.Int64(partNumber),
		ContentLength: aws.Int64(contentLength),
	}

	res, err := s.c.UploadPart(pi)
	if err != nil {
		return nil, err
	}

	return &s3.CompletedPart{
		ETag:       res.ETag,
		PartNumber: aws.Int64(partNumber),
	}, nil
}

func (f *FileInfo) Name() string { return f.name }

func (f *FileInfo) Size() int64 { return f.size }

func (f *FileInfo) ModTime() time.Time { return f.mtime }

func (f *FileInfo) IsDir() bool { return f.isdir }
