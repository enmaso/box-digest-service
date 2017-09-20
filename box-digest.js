require('dotenv').config()

const mongoose = require('mongoose')
const BoxSDK = require('box-node-sdk')
const AWS = require('aws-sdk')
const Consumer = require('sqs-consumer')
const async = require('async')
const exec = require('child_process').exec
const fs = require('fs')
const net = require('net')

const File = require('./lib/file')
const Service = require('./lib/service')

const TIKA_META = `${process.env.TIKA_HOST}/meta`
const TIKA_TEXT = `${process.env.TIKA_HOST}/tika`

// Mongo connection
let mongoOpts = {
  poolSize: Number(process.env.MONGO_POOLSIZE) || 4,
  useMongoClient: true
}
let mongoUri = `mongodb://${process.env.MONGO_HOST}/${process.env.MONGO_DB}`
mongoose.connect(mongoUri, mongoOpts)

const sqs = new AWS.SQS({
  apiversion: '2012-11-05',
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION
})

const app = Consumer.create({
  queueUrl: process.env.AWS_QUEUE_URL,
  terminateVisibilityTimeout: true,
  sqs: sqs,
  handleMessage: (message, done) => {
    processFile(message, (err) => {
      if (err) {
        done(err)
      } else {
        done()
      }
    })
  }
})

app.on('error', (err) => {
  console.error(err.message)
})

console.log('Starting Box Digest Service')
app.start()

function processFile (message, done) {
  let fileId = message.Body
  console.log('Downloading file', fileId)
  File.findById(fileId)
  .then(file => {
    Service.findById(file.serviceId)
    .then(service => {
      // Load box-sdk client
      let sdk = new BoxSDK({
        clientID: process.env.BOX_CLIENT_ID,
        clientSecret: process.env.BOX_CLIENT_SECRET
      })
      // Refresh access tokens if needed
      sdk.getTokensRefreshGrant(service.refreshToken, (err, tokens) => {
        if (err) {
          console.error(err)
          return done(err)
        } else {
          service.accessToken = tokens.accessToken
          service.refreshToken = tokens.refreshToken
          service.save(err => {
            if (err) {
              console.error(err)
            }
          })
          let client = sdk.getBasicClient(service.accessToken)
          // Download the file
          client.files.getReadStream(file.source.id, null, (err, stream) => {
            if (err) {
              console.error(err)
              return done(err)
            } else {
              // PROCESS FILE START
              let tmp = `/tmp/${file._id.toString()}${file.name.substring(file.name.lastIndexOf('.'))}`
              let fwrite = fs.createWriteStream(tmp)
              stream.pipe(fwrite)
              fwrite.on('close', () => {
                async.series([
                  function (next) {
                    // Extract file metadata with Tika
                    exec(`curl -T ${tmp} ${TIKA_META} --header 'Accept: application/json'`, (err, stdout, stderr) => {
                      if (err) {
                        console.error(err)
                      } else {
                        file.metadata = JSON.parse(stdout)
                        file.save(err => {
                          if (err) {
                            console.log(err)
                          }
                        })
                      }
                      next()
                    })
                  },
                  function (next) {
                    // Extract file text with Tika
                    console.log(`Extracting text from ${tmp}`)
                    exec(`curl -T ${tmp} ${TIKA_TEXT}`, (err, stdout, stderr) => {
                      if (err) {
                        console.error(err)
                      } else {
                        file.text = stdout.replace(/\r?\n|\r|\t/g, ' ').trim()
                        file.save(err => {
                          if (err) {
                            console.error(err)
                          }
                        })
                      }
                      next()
                    })
                  },
                  function (next) {
                    // Extract NER from file text
                    if (file.text.length === 0) {
                      console.log(`No text to be extracted from ${tmp}`)
                      return next()
                    }
                    let socket = new net.Socket()
                    socket.connect(process.env.NER_PORT, 'localhost', () => {
                      socket.write(file.text + '\n')
                    })
                    socket.on('data', data => {
                      let regexp = /<([A-Z]+?)>(.+?)<\/\1>/g
                      let str = data.toString()
                      let tags = {
                        LOCATION: [],
                        ORGANIZATION: [],
                        DATE: [],
                        MONEY: [],
                        PERSON: [],
                        PERCENT: [],
                        TIME: []
                      }
                      let m
                      while ((m = regexp.exec(str)) !== null) {
                        if (m.index === regexp.lastIndex) {
                          regexp.lastIndex++
                        }
                        tags[m[1]].push(m[2])
                      }
                      socket.destroy()
                      file.ner = tags
                      file.save(err => {
                        if (err) {
                          console.error(err)
                        }
                      })
                      next()
                    })
                  },
                  function (next) {
                    // Convert file to image, if possible
                    switch (file.metadata['Content-Type']) {
                      case 'application/pdf': {
                        let img = `/tmp/${file._id.toString()}.png`
                        exec(`convert ${tmp}[0] ${img}`, (err, stdout, stderr) => {
                          if (err) {
                            console.error(err)
                          }
                          // Move the image to s3
                          next()
                        })
                      } break
                      case 'text/plain':
                      case 'text/csv':
                      case 'application/msword':
                      case 'application/vnd.ms-excel':
                      case 'application/vnd.ms-powerpoint':
                      case 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
                      case 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
                      case 'application/vnd.openxmlformats-officedocument.presentationml.presentation': {
                        let img = `/tmp/${file._id.toString()}.png`
                        let pdf = `/tmp/${file._id.toString()}.pdf`
                        exec(`unoconv -f pdf ${tmp}`, (err, stdout, stderr) => {
                          if (err) {
                            console.error(err)
                            next()
                          } else {
                            exec(`convert ${pdf}[0] ${img}`, (err, stdout, stderr) => {
                              if (err) {
                                console.error(err)
                              }
                              // Move the image to s3

                              // If a pdf exists, delete it
                              if (fs.existsSync(pdf)) {
                                fs.unlinkSync(pdf)
                              }
                              next()
                            })
                          }
                        })
                      } break
                      default: {
                        next()
                      }
                    }
                  }
                ], (err, results) => {
                  if (err) {
                    console.error(err)
                  }
                  done()
                  fs.unlinkSync(tmp)
                })
              })
              // PROCESS FILE END
            }
          })
        }
      })
    })
    .catch(err => {
      console.error(err)
      done(err)
    })
  })
  .catch(err => {
    console.error(err)
    done(err)
  })
}
