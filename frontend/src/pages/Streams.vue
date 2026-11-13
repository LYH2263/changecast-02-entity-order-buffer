<script setup>
import { onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'

const rows = ref([])
const err = ref('')
const saved = ref('')
const editId = ref(null)
const form = ref({ buffer_max_events: 0, buffer_max_wait_seconds: 0, start_seq: 1 })

async function refresh() {
  rows.value = await getJSON('/api/streams')
}

function startEdit(s) {
  editId.value = s.id
  saved.value = ''
  form.value = {
    buffer_max_events: s.buffer_max_events,
    buffer_max_wait_seconds: s.buffer_max_wait_seconds,
    start_seq: s.start_seq,
  }
}

async function save() {
  err.value = ''
  saved.value = ''
  try {
    await postJSON(`/api/streams/${editId.value}/config`, {
      buffer_max_events: Number(form.value.buffer_max_events),
      buffer_max_wait_seconds: Number(form.value.buffer_max_wait_seconds),
      start_seq: Number(form.value.start_seq),
    })
    saved.value = '已保存。新配置对下一趟运行生效。'
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  }
}

onMounted(async () => {
  try { await refresh() } catch (e) { err.value = String(e.message || e) }
})
</script>
<template>
  <div class="page">
    <h1>投影流</h1>
    <p class="muted">
      每条流绑定 CDC 夹具、实体键名与载荷字段。缓冲上限（条数 / 等待时长）与起始 seq 按流可配；
      逾限策略全局选定「阻塞该实体」（block），不丢弃事件。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="panel">
      <table>
        <thead>
          <tr>
            <th>ID</th><th>编码</th><th>标题</th><th>实体键</th><th>夹具</th>
            <th>缓冲上限</th><th>等待上限(s)</th><th>起始 seq</th><th>策略</th><th>水位</th><th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="s in rows" :key="s.id">
            <td>{{ s.id }}</td>
            <td class="mono">{{ s.code }}</td>
            <td>{{ s.title }}</td>
            <td class="mono">{{ s.entity_key_name }}</td>
            <td class="mono">{{ s.fixture }}</td>
            <td class="mono">{{ s.buffer_max_events }}</td>
            <td class="mono">{{ s.buffer_max_wait_seconds }}</td>
            <td class="mono">{{ s.start_seq }}</td>
            <td><span class="tag danger">{{ s.overflow_policy }}</span></td>
            <td class="mono">{{ s.watermark_seq }}</td>
            <td><button @click="startEdit(s)">配置</button></td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="panel" v-if="editId">
      <h3 style="margin-top:0">编辑缓冲配置 · {{ rows.find(r => r.id === editId)?.title }}</h3>
      <div style="display:flex;flex-wrap:wrap;gap:0.75rem;align-items:flex-end">
        <label>
          缓冲上限（条/实体）
          <input v-model.number="form.buffer_max_events" type="number" min="0" style="display:block;width:9rem;margin-top:0.25rem" />
        </label>
        <label>
          等待上限（秒/实体）
          <input v-model.number="form.buffer_max_wait_seconds" type="number" min="0" style="display:block;width:9rem;margin-top:0.25rem" />
        </label>
        <label>
          起始 seq（新实体期望）
          <input v-model.number="form.start_seq" type="number" min="0" style="display:block;width:9rem;margin-top:0.25rem" />
        </label>
        <button @click="save">保存配置</button>
      </div>
      <p class="muted" style="margin:0.6rem 0 0">
        实体缓冲条数 &gt; 上限，或最老缓冲等待 ≥ 上限，即触发逾限 → 该实体阻塞（后续事件滞留），其他实体不受影响。
      </p>
      <p v-if="saved" class="tag ok" style="margin:0.5rem 0 0">{{ saved }}</p>
    </div>
  </div>
</template>
