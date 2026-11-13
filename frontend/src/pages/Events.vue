<script setup>
import { onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { getJSON } from '../api'

const route = useRoute()
const rows = ref([])
const batchId = ref(null)
const result = ref('')
const err = ref('')

const RESULTS = [
  ['', '全部结果'],
  ['applied_upsert', 'applied_upsert 即刻写入'],
  ['applied_delete', 'applied_delete 即刻删除'],
  ['released_upsert', 'released_upsert 补齐放出'],
  ['released_delete', 'released_delete 补齐放出(删)'],
  ['flushed_upsert', 'flushed_upsert 清缓冲放出'],
  ['flushed_delete', 'flushed_delete 清缓冲放出(删)'],
  ['buffered', 'buffered 入缓'],
  ['held_blocked', 'held_blocked 阻塞滞留'],
  ['skipped_stale', 'skipped_stale 过期跳过'],
  ['skipped_duplicate', 'skipped_duplicate 重复跳过'],
  ['skipped_redundant', 'skipped_redundant 冗余跳过'],
]

function tagClass(r) {
  if (r.startsWith('applied') || r.startsWith('released') || r.startsWith('flushed')) return 'ok'
  if (r === 'held_blocked' || r === 'skipped_stale') return 'danger'
  return 'warn'
}

async function refresh() {
  const q = new URLSearchParams()
  if (batchId.value) q.set('batch_id', String(batchId.value))
  if (result.value) q.set('result', result.value)
  rows.value = await getJSON('/api/events?' + q.toString())
}

onMounted(async () => {
  try {
    if (route.query.batch_id) batchId.value = Number(route.query.batch_id)
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  }
})

watch([batchId, result], async () => {
  try { await refresh() } catch (e) { err.value = String(e.message || e) }
})
</script>
<template>
  <div class="page">
    <h1>事件留痕</h1>
    <p class="muted">
      每个源事件的确切裁决：即刻应用 / 入缓 / 阻塞滞留 / 过期 / 重复，以及缓冲事件后来的放出（补齐或清缓冲）。
      同一 event_id 可能留下「入缓 → 放出」两行，即其完整生命周期。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="panel" style="display:flex;gap:0.75rem;align-items:flex-end;flex-wrap:wrap">
      <label>
        批次 ID（可选）
        <input v-model.number="batchId" type="number" style="display:block;margin-top:0.25rem;width:8rem" placeholder="全部" />
      </label>
      <label>
        结果
        <select v-model="result" style="display:block;margin-top:0.25rem;min-width:14rem">
          <option v-for="[v, label] in RESULTS" :key="v" :value="v">{{ label }}</option>
        </select>
      </label>
      <button @click="batchId = null; result = ''; refresh()">看全部</button>
    </div>
    <div class="panel">
      <table>
        <thead>
          <tr>
            <th>批次</th><th>event_id</th><th>实体键</th><th>op</th><th>seq</th><th>事件时间</th><th>结果</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="e in rows" :key="e.id">
            <td>{{ e.batch_id }}</td>
            <td class="mono">{{ e.event_id }}</td>
            <td class="mono">{{ e.entity_key }}</td>
            <td class="mono">{{ e.op }}</td>
            <td class="mono">{{ e.seq }}</td>
            <td class="mono">{{ e.event_time }}</td>
            <td><span class="tag" :class="tagClass(e.result)">{{ e.result }}</span></td>
          </tr>
          <tr v-if="!rows.length"><td colspan="7" class="muted">暂无留痕。</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
