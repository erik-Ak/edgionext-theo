import functions from '@google-cloud/functions-framework'
import { Client } from '@elastic/elasticsearch'
import zlib from 'zlib'
import axios from 'axios'

export default async function handler(req, res) {
  try {
    // Check for access token
    if (req.query.token == null || req.query.token !== '5MbOHiS.b2KrIT.8nQ9HsYbm') {
      res.status(403).send('')
    }

    // Check for type parameter
    const type = req.query.type
    if (type == null) {
      res.status(400).send('Invalid query parameters')
    }

    const json = req.body

    if (typeof json.logs === 'undefined') {
      throw 'Invalid request body ' + JSON.stringify(json)
    }

    // Determine elastic search index depending on type
    let elasticIndex = ''
    if (type === 'adn') {
      elasticIndex = 'adn_log_raw'
    } else if (type === 'rate') {
      elasticIndex = 'rate_limiting_raw'
    } else if (type === 'event') {
      elasticIndex = 'event_log_raw'
    } else if (type === 'cdn') {
      elasticIndex = 'cdn_log_raw'
    } else if (type === 'bot') {
      elasticIndex = 'bot_raw_log'
    } else {
      throw 'Unknown type'
    }

    const data = {}
    data['body'] = []
    json.logs.forEach((item) => {
      item.timestamp = new Date(item.timestamp * 1000)
      item.created_at = Date.now()
      data['body'].push({
        index: {
          _index: elasticIndex,
        },
      })
      data['body'].push(item)
    })

    const client = new Client({
      node: 'https://185.141.192.64:9200',
      auth: {
        username: 'admin',
        password: 'Q2tjg.BkAhxNs3YuE.HgWxyK',
      },
      tls: {
        rejectUnauthorized: false,
      },
    })

    const elasticRes = await client.bulk(data)

    if (elasticRes && elasticRes.errors === false) {
      res.status(200).send('Success')
    } else {
      const errorSlackRes = await axios.post('https://hooks.slack.com/services/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', {
        text: 'Error in Elasticsearch ' + JSON.stringify(elasticRes ? elasticRes : []),
      })
      res.status(500).send(JSON.stringify(elasticRes))
    }
  } catch (error) {
    const errorSlackRes = await axios.post('https://hooks.slack.com/services/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', {
      text: 'Error in Google Function ' + error,
    })
    res.status(500).send(JSON.stringify(error))
  }
}
