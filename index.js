var AWS = require('aws-sdk')
let s3 = new AWS.S3({apiVersion: '2006-03-01'})
var bucket = 'hudlrd-experiments'
var bucketParams = {
  Bucket: bucket
}

// Call S3 to create the bucket
s3.listObjects(bucketParams, function (err, data) {
  if (err) {
    console.log('Error', err)
  } else {
    for (let file of data.Contents) {
      let keyParts = file.Key.split('/')

      if (keyParts.length === 5) {
        let resultFileParts = keyParts[4].split('.')
        let fileExtension = resultFileParts[1]

        if (fileExtension === 'json') {
          let variableParts = keyParts[2].split('-')
          let resultParts = keyParts[4].split('-')
          let dataSet = resultFileParts[0].split(/_(.+)/)[0]
          let resultType = resultFileParts[0].split(/_(.+)/)[1]
          let experimentName = keyParts[1]
          let param = variableParts[0].split(/_(.+)/)[1]
          let value = variableParts[1]
          let revision = variableParts[0].split(/_(.+)/)[0]

          let newKey = `test-results/dataset=${dataSet}/result_type=${resultType}/experiment=${experimentName}/adjusted_variable=${param}/variable_value=${value}/${revision}.json`

          copyObject(
            s3,
            `${file.Key}`,
            `${newKey}`
          )
        }
      }
    }
  }
})

// AWS Athena only allows one json object per line so we need to manipulate the file locally before
// uploading again
function copyObject (s3, oldKey, newKey) {
  var params = {
    Bucket: bucket,
    Key: oldKey
  }
  s3.getObject(params, function (err, data) {
    if (err) console.log(err, err.stack) // an error occurred
    else {
      let output = JSON.stringify(JSON.parse(data.Body.toString('utf-8')))
      console.log(`Got data from ${oldKey}`)
      s3.putObject({
        Body: output,
        Bucket: bucket,
        Key: newKey
      }, (err, data) => {
        console.log(`Saved data to ${newKey}`)
      })
    }
  })
}
