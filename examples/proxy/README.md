## Minio run instructions

To run minio as similar as s3 it's required to run with https, to be able to do that you would need to create ssh keys. 
The example has key created that are valid for 127.0.0.1 and localhost but you will need to create new ssh keys.

To create new ssh keys you will need to run the following commands. (OS X)
```
brew install mkcert
```

```
brew install nss
```

```
mkcert -install
```
Then you can create the keys, Minio expects this exact files also you need to create a copy of the public key in a folder called `CAs`, as done in this example.

```
mkcert -cert-file public.crt -key-file private.key 127.0.0.1 localhost
```

Then in the docker-compose file mount the certificates to the directory `/certs`
```
    volumes:
      - ${PWD}/minio_data:/data
      - ${PWD}/examples/proxy/certs:/certs
```

Once the minio instance is up create a bucket called `metricstore`.

