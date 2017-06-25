"use strict";

const gcsstorage = require("@google-cloud/storage");
const stream = require("stream");
const intoStream = require("into-stream");
const fs = require("fs");
const zlib = require("zlib");
import retry from "async-retry";

interface IStorage {
    save(gcsPath: string, file: string): Promise<any>;
    read(gcsPath: string, writableStream: any): Promise<any>;
    readAsObject(gcsPath: string, options?: any): Promise<Object>;
    readAsBuffer(gcsPath: string, options?: any): Promise<Buffer>;
    list(prefix: string, options?: any): Promise<any>;
    delete(gcsPath: string): Promise<any>;
}

interface IGCSCredentials {
  client_email: string;
  private_key: string;
}

interface IgscConfig {
    projectId: string;
    keyFilename?: string;
    credentials?: IGCSCredentials;
}

interface Ioptions {
    loggingFunction?: any;
    bucket?: string;
    retriesCount?: number;
    retryInterval?: number;
    maxRetryTimeout?: number;
}

interface IFile {
    fullBlobName: string;
    properties: any;
    metadata: any;
}

class GoogleCloudStorage implements IStorage {
    log: (...args) => void;
    gscConfig: any;
    storage: any;
    bucket: string;
    retriesCount: number;
    retryInterval: number;
    maxRetryTimeout: number;

    constructor(config: IgscConfig, options: Ioptions) {
        options = options || {};
        if (!config || !config.projectId || !config.keyFilename && !config.credentials) {
            throw new Error("Configuration object is invalid.");
        }
        if (config.credentials && config.credentials.private_key) {
          config.credentials.private_key = config.credentials.private_key.replace(/\\n/g, "\n");
        }
        this.bucket = options.bucket;
        this.storage = gcsstorage(config);
        this.log = options.loggingFunction || (() => null);
        this.retriesCount = options.retriesCount || 3;
        this.retryInterval = options.retryInterval || 500;
        this.maxRetryTimeout = options.maxRetryTimeout || 90000;
    }

    private getRemoteFileInstance (gcsPath: string): any {
      return this.storage.bucket(this.bucket).file(gcsPath);
    }

    async list(prefix: string, queryOptions?: any): Promise<IFile[]> {
        let query = {
            prefix: prefix || "",
            autoPaginate: false
        };
        // Expand query by additional parameters.
        // DOCS: https://googlecloudplatform.github.io/google-cloud-node/#/docs/storage/0.5.0/storage/bucket?method=getFiles
        if (queryOptions) {
          for (let option in queryOptions) {
            query[option] = queryOptions[option];
          }
        }
        let aStream = this.storage.bucket(this.bucket).getFilesStream(query);
        return this.readableStreamToPromise(aStream)
            .then((files) => {
                return files.map(this.transformGCSFileToFileInPrefferedFormat);
            });
    }

    private transformGCSFileToFileInPrefferedFormat (file: any): IFile {
      return {
        fullBlobName: file.name,
        properties: file.metadata || {},
        metadata: file.metadata.metadata || {}
      };
    }

    async delete(gcsPath: string): Promise<any> {
        let deletedFile = await this.getRemoteFileInstance(gcsPath).delete();
        return !!deletedFile;
    }

    private transformToStream(data: any) {
        if (data instanceof stream.Readable) {
            return data;
        } else if (Buffer.isBuffer(data)) {
            return this.convertBufferToStream(data);
        } else if (data instanceof Object) {
            return this.convertObjectIntoStream(data);
        } else if (typeof data === "string") {
            return fs.createReadStream(data);
        } else {
            throw new Error("Specified parameter has unsupported type.");
        }
    }

    async save(gcsPath: string, data: any, options?: any): Promise<any> {
        options = options || {};
        let rStream = null;
        let gcsStream = null;
        let aFile = this.getRemoteFileInstance(gcsPath);
        let gcsUploadOptions = {
            gzip: options.compress === true,
            public: true, // Make it optinal
            metadata: {
                contentType: options.contentType || "application/json",
                metadata: options.metadata || {}
            },
            resumable: false,
            validation: "crc32c"
        };

        return this.tryToDoOrFail(() => {
            let urlToFile = (options.getURL) ? this.buildUrlToFile(gcsPath) : null;
            rStream = this.transformToStream(data);
            gcsStream = aFile.createWriteStream(gcsUploadOptions);
            return this.uploadFile(rStream, gcsStream, urlToFile);
        }, {
          onRetry: function (error) {
            if (rStream && gcsStream) {
              rStream.unpipe(gcsStream);
              gcsStream.end();
            }
          }
        });
    }

    private buildUrlToFile (gcsPath: string): string {
      return "https://" + this.bucket + ".storage.googleapis.com/" + gcsPath;
    }

    private async uploadFile(readableStream: any, writableStream: any, urlToFile: any) {
        return new Promise((resolve, reject) => {
            readableStream
                .pipe(writableStream)
                .on("error", function(err) {
                    reject(new Error(err));
                })
                .on("finish", function() {
                    resolve(urlToFile || true);
                });
        });
    }

    async read (gcsPath: string, writableStream: any): Promise<any> {
        return this.readAsBuffer(gcsPath).then((buffer) => {
          return new Promise ((resolve, reject) => {
            intoStream(buffer)
                    .pipe(writableStream)
                    .on("finish", resolve)
                    .on("error", reject);
            });
        });
    }

    async readAsObject(gcsPath: string, options?: any): Promise<Object> {
      return this.readAsBuffer(gcsPath, options).then((buffer) => {
          let json = buffer.toString("utf8");
          try {
            return JSON.parse(json); 
          } catch (e) {
            // Let's suppose that if not JSON then zipped file -> testRTC specific check
            // TODO: add check for zipped format
            let unzipped = zlib.unzipSync(buffer);
            return JSON.parse(unzipped.toString('utf8'));
          }
      });
    }

    async readAsBuffer(gcsPath: string, options?: any): Promise<Buffer> {
      return this.tryToDoOrFail(() => {
        return this.getRemoteFileInstance(gcsPath)
                   .download()
                   .then((data) => data[0])
                   .catch((error) => {
                     this.log(`'getRemoteFileInstance' failed with the reason, ${error}`);
                     throw new Error(error)
                   });
      });
    }

    private async readableStreamToPromise(readableStream): Promise<any> {
        return new Promise((resolve, reject) => {
            let data = [];
            readableStream
                .on("error", reject)
                .on("data", (chunk) => {
                    data.push(chunk);
                })
                .on("end", () => {
                    resolve(data);
                });
        });
    }

    private convertObjectIntoStream(object: any): any {
        let stringifiedJson = JSON.stringify(object, null, 0);
        let buffer = new Buffer(stringifiedJson, "utf8");
        return this.convertBufferToStream(buffer);
    }

    private convertBufferToStream(buffer: Buffer) {
        return intoStream(buffer);
    }

    private async delay(timeout) {
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                this.log(`DELAY - ${timeout}, happend at ${new Date().toISOString()}`);
                reject(new Error("Promise did not get final state in max retry timeout."));
            }, timeout);
        });
    }

    /**
    *   If promise does not get final state in max retry timeout, reject it with timeout error.
    */
    private async limitPromiseTime (asyncOperation) {
      return Promise.race([
        asyncOperation(),
        this.delay(this.maxRetryTimeout)
      ]);
    }

    async tryToDoOrFail(asyncOperation, options?) {
        let counter = this.retriesCount;
        this.log(`Async operation started at ${new Date().toISOString()}, retries left on start ${this.retriesCount}`);
        return await retry(this.limitPromiseTime.bind(this, asyncOperation), {
          retries: 3,
          minTimeout: 1000,
          onRetry: (error) => {
            if (options && options.onRetry) {
                options.onRetry(error);
            }
            counter--;
            this.log(`Error while saving blob: '${error}'. Retries left: ${counter}, happend at ${new Date().toISOString()}`);
          }
        });
    }
}

export = GoogleCloudStorage;
