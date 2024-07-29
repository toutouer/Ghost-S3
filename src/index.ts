import {
  type PutObjectCommandInput,
  type S3ClientConfig,
  type ObjectCannedACL,
  S3,
} from '@aws-sdk/client-s3'
import StorageBase, { type ReadOptions, type Image } from 'ghost-storage-base'
import { join } from 'path'
import { createReadStream } from 'fs'
import type { Readable } from 'stream'
import type { Handler } from 'express'

const stripLeadingSlash = (s: string) =>
  s.indexOf('/') === 0 ? s.substring(1) : s
const stripEndingSlash = (s: string) =>
  s.indexOf('/') === s.length - 1 ? s.substring(0, s.length - 1) : s

type Config = {
  accessKeyId?: string
  assetHost?: string
  bucket?: string
  pathPrefix?: string | { [key: string]: string }
  region?: string
  secretAccessKey?: string
  endpoint?: string
  forcePathStyle?: boolean
  acl?: string
}

class S3Storage extends StorageBase {
  accessKeyId?: string
  secretAccessKey?: string
  region?: string
  bucket?: string
  host: string
  pathPrefix: string | { [key: string]: string }
  endpoint: string
  forcePathStyle: boolean
  acl?: ObjectCannedACL

  constructor(config: Config = {}) {
    super()

    const {
      accessKeyId,
      assetHost,
      bucket,
      pathPrefix,
      region,
      secretAccessKey,
      endpoint,
      forcePathStyle,
      acl,
    } = config

    this.accessKeyId = accessKeyId
    this.secretAccessKey = secretAccessKey
    this.region = process.env.AWS_DEFAULT_REGION || region

    this.bucket = process.env.GHOST_STORAGE_ADAPTER_S3_PATH_BUCKET || bucket

    if (!this.bucket) throw new Error('S3 bucket not specified')

    this.forcePathStyle =
      Boolean(process.env.GHOST_STORAGE_ADAPTER_S3_FORCE_PATH_STYLE) ||
      Boolean(forcePathStyle) ||
      false

    let defaultHost: string

    if (this.forcePathStyle) {
      defaultHost = `https://s3${
        this.region === 'us-east-1' ? '' : `.${this.region}`
      }.amazonaws.com/${this.bucket}`
    } else {
      defaultHost = `https://${this.bucket}.s3${
        this.region === 'us-east-1' ? '' : `.${this.region}`
      }.amazonaws.com`
    }

    this.host =
      process.env.GHOST_STORAGE_ADAPTER_S3_ASSET_HOST ||
      assetHost ||
      defaultHost

    this.pathPrefix = process.env.GHOST_STORAGE_ADAPTER_S3_PATH_PREFIX || pathPrefix || ''
    if (typeof this.pathPrefix === 'string') {
      this.pathPrefix = stripLeadingSlash(this.pathPrefix)
    } else {
      Object.keys(this.pathPrefix).forEach(key => {
        this.pathPrefix[key] = stripLeadingSlash(this.pathPrefix[key])
      })
    }
    this.endpoint =
      process.env.GHOST_STORAGE_ADAPTER_S3_ENDPOINT || endpoint || ''
    this.acl = (process.env.GHOST_STORAGE_ADAPTER_S3_ACL ||
      acl ||
      'public-read') as ObjectCannedACL
  }

  private getPathPrefix(fileType: string): string {
    if (typeof this.pathPrefix === 'string') {
      return this.pathPrefix
    }
    return this.pathPrefix[fileType] || ''
  }

  private getFileType(fileName: string): string {
    const ext = fileName.split('.').pop()?.toLowerCase()
    if (['jpg', 'jpeg', 'png', 'gif', 'svg'].includes(ext)) return 'images'
    if (['mp4', 'webm', 'ogg'].includes(ext)) return 'media'
    return 'files'
  }

  async delete(fileName: string, targetDir?: string) {
    const fileType = this.getFileType(fileName)
    const pathPrefix = this.getPathPrefix(fileType)
    const directory = targetDir || this.getTargetDir(pathPrefix)

    try {
      await this.s3().deleteObject({
        Bucket: this.bucket,
        Key: stripLeadingSlash(join(directory, fileName)),
      })
    } catch {
      return false
    }
    return true
  }

  async exists(fileName: string, targetDir?: string) {
    const fileType = this.getFileType(fileName)
    const pathPrefix = this.getPathPrefix(fileType)
    try {
      await this.s3().getObject({
        Bucket: this.bucket,
        Key: stripLeadingSlash(
          targetDir ? join(targetDir, fileName) : join(pathPrefix, fileName)
        ),
      })
    } catch {
      return false
    }
    return true
  }

  s3() {
    const options: S3ClientConfig = {
      region: this.region,
      forcePathStyle: this.forcePathStyle,
    }

    if (this.accessKeyId && this.secretAccessKey) {
      options.credentials = {
        accessKeyId: this.accessKeyId,
        secretAccessKey: this.secretAccessKey,
      }
    }

    if (this.endpoint !== '') {
      options.endpoint = this.endpoint
    }
    return new S3(options)
  }

  urlToPath(url: string) {
    const parsedUrl = new URL(url)
    return parsedUrl.pathname
  }

  async save(image: Image, targetDir?: string) {
    const fileType = this.getFileType(image.name)
    const pathPrefix = this.getPathPrefix(fileType)
    const directory = targetDir || this.getTargetDir(pathPrefix)

    const fileName = await this.getUniqueFileName(image, directory)
    const file = createReadStream(image.path)

    let config: PutObjectCommandInput = {
      ACL: this.acl,
      Body: file,
      Bucket: this.bucket,
      CacheControl: `max-age=${30 * 24 * 60 * 60}`,
      ContentType: image.type,
      Key: stripLeadingSlash(fileName),
    }
    await this.s3().putObject(config)

    return `${this.host}/${stripLeadingSlash(fileName)}`
  }

  serve(): Handler {
    return async (req, res, next) => {
      try {
        const fileType = this.getFileType(req.path)
        const pathPrefix = this.getPathPrefix(fileType)
        const key = stripLeadingSlash(stripEndingSlash(pathPrefix) + req.path)
        const output = await this.s3().getObject({
          Bucket: this.bucket,
          Key: key,
        })

        const headers: { [key: string]: string } = {}
        if (output.AcceptRanges) headers['accept-ranges'] = output.AcceptRanges
        if (output.CacheControl) headers['cache-control'] = output.CacheControl
        if (output.ContentDisposition)
          headers['content-disposition'] = output.ContentDisposition
        if (output.ContentEncoding)
          headers['content-encoding'] = output.ContentEncoding
        if (output.ContentLanguage)
          headers['content-language'] = output.ContentLanguage
        if (output.ContentLength)
          headers['content-length'] = `${output.ContentLength}`
        if (output.ContentRange) headers['content-range'] = output.ContentRange
        if (output.ContentType) headers['content-type'] = output.ContentType
        if (output.ETag) headers['etag'] = output.ETag
        res.set(headers)

        const stream = output.Body as Readable
        stream.pipe(res)
      } catch (err) {
        res.status(404)
        next(err)
      }
    }
  }

  async read(options: ReadOptions = { path: '' }) {
    let path = (options.path || '').replace(/\/$|\\$/, '')

    if (!path.startsWith(this.host)) {
      throw new Error(`${path} is not stored in s3`)
    }
    path = path.substring(this.host.length)

    const response = await this.s3().getObject({
      Bucket: this.bucket,
      Key: stripLeadingSlash(path),
    })
    const stream = response.Body as Readable

    return await new Promise<Buffer>((resolve, reject) => {
      const chunks: Buffer[] = []
      stream.on('data', (chunk) => chunks.push(chunk))
      stream.once('end', () => resolve(Buffer.concat(chunks)))
      stream.once('error', reject)
    })
  }
}

export default S3Storage
