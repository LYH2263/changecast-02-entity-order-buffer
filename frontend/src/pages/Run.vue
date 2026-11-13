<script setup>
import { onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'

const streams = ref([])
const batches = ref([])
const streamId = ref(null)
const note = ref('')
const fixture = ref('')
const last = ref(null)
const busy = ref(false)
const err = ref('')

async function refresh() {
  streams.value = await getJSON('/api/streams')
  batches.value = await getJSON('/api/batches?limit=20')
  if (streamId.value == null && streams.value.length) streamId.value = streams.value[0].id
}

async function run() {
  err.value = ''
  busy.value = true
  try {
    last.value = await postJSON('/api/project', {
      stream_id: Number(streamId.value),
      note: note.value || null,
      fixture: fixture.value.trim() || null,
    })
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

onMounted(async () => {
  try { await refresh() } catch (e) { err.value = String(e.message || e) }
})
</script>
<template>
  <div class="page">
    <h1>投影台</h1>
    <p class="muted">
      选一条投影流，按<b>到达顺序</b>逐条过实体保序闸门：seq 命中期望才改投影，超前入缓，迟到判过期。
      各实体的期望 seq、缓冲与阻塞态跨批次保持；重跑同一夹具只会得到重复跳过。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>

    <div class="panel">
      <h3 style="margin-top:0">发起投影</h3>
      <div style="display:flex;flex-wrap:wrap;gap:0.75rem;align-items:flex-end">
        <label>
          投影流
          <select v-model.number="streamId" style="display:block;min-width:16rem;margin-top:0.25rem">
            <option v-for="s in streams" :key="s.id" :value="s.id">{{ s.title }}（{{ s.code }}）</option>
          </select>
        </label>
        <label>
          夹具覆盖（可选）
          <input v-model="fixture" style="display:block;width:15rem;margin-top:0.25rem" placeholder="默认流夹具；补齐填 *_fill.json" />
        </label>
        <label>
          备注（可选）
          <input v-model="note" style="display:block;width:12rem;margin-top:0.25rem" placeholder="本趟说明" />
        </label>
        <button :disabled="busy || !streamId" @click="run">{{ busy ? '投影中…' : '应用变更流' }}</button>
      </div>
      <p class="muted" style="margin:0.6rem 0 0">
        演示逾限：先跑 <span class="mono">reorder_lab</span> 默认夹具，再用 <span class="mono">reorder_lab_fill.json</span> 覆盖跑一趟补齐缺口。
      </p>
    </div>

    <div class="panel" v-if="last">
      <h3 style="margin-top:0">最近一趟</h3>
      <p>
        批次 <span class="mono">{{ last.batch_no }}</span> · {{ last.stream_title }} · 夹具 <span class="mono">{{ last.fixture }}</span> ·
        <span class="tag ok">{{ last.status }}</span>
        <span class="tag" :class="last.reconciled ? 'ok' : 'danger'">{{ last.reconciled ? '计数互证通过' : '计数不符' }}</span>
      </p>
      <div class="stat-grid">
        <div class="stat"><div class="stat-num">{{ last.source_events }}</div><div class="stat-label">源事件</div></div>
        <div class="stat"><div class="stat-num">{{ last.applied_upsert + last.applied_delete }}</div><div class="stat-label">即刻应用</div></div>
        <div class="stat"><div class="stat-num">{{ last.released_upsert + last.released_delete }}</div><div class="stat-label">缓冲放出</div></div>
        <div class="stat"><div class="stat-num">{{ last.buffered }}</div><div class="stat-label">入缓</div></div>
        <div class="stat"><div class="stat-num">{{ last.held_blocked }}</div><div class="stat-label">阻塞滞留</div></div>
        <div class="stat"><div class="stat-num">{{ last.skipped_stale }}</div><div class="stat-label">过期跳过</div></div>
        <div class="stat"><div class="stat-num">{{ last.skipped_duplicate }}</div><div class="stat-label">重复跳过</div></div>
        <div class="stat"><div class="stat-num">{{ last.overflow_blocks }}</div><div class="stat-label">新增阻塞</div></div>
        <div class="stat"><div class="stat-num">{{ last.buffer_open_after }}</div><div class="stat-label">缓冲余量</div></div>
        <div class="stat"><div class="stat-num">{{ last.blocked_entities_after }}</div><div class="stat-label">阻塞实体</div></div>
      </div>
      <div v-if="last.reasons && last.reasons.length" style="margin-top:0.6rem">
        <p v-for="(r, i) in last.reasons" :key="i" class="tag warn" style="margin:0.2rem 0">{{ r }}</p>
      </div>
      <p style="margin-top:0.75rem">
        <router-link :to="{ path: '/projections', query: { stream_id: last.stream_id } }">查看投影快照 →</router-link>
        ·
        <router-link :to="{ path: '/events', query: { batch_id: last.id } }">事件留痕 →</router-link>
        ·
        <router-link to="/buffer">缓冲区 →</router-link>
      </p>
    </div>

    <div class="panel">
      <h3 style="margin-top:0">历史批次</h3>
      <table>
        <thead>
          <tr>
            <th>批次</th><th>投影流</th><th>类型</th>
            <th>源事件</th><th>应用</th><th>放出</th><th>入缓</th><th>滞留</th><th>过期</th><th>阻塞</th><th>时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="b in batches" :key="b.id">
            <td class="mono">{{ b.batch_no }}</td>
            <td>{{ b.stream_title }}</td>
            <td><span class="tag" :class="b.kind === 'flush' ? 'warn' : ''">{{ b.kind === 'flush' ? '清缓冲' : '运行' }}</span></td>
            <td>{{ b.source_events }}</td>
            <td>{{ b.applied_upsert + b.applied_delete }}</td>
            <td>{{ b.released_upsert + b.released_delete + b.flushed_upsert + b.flushed_delete }}</td>
            <td>{{ b.buffered }}</td>
            <td>{{ b.held_blocked }}</td>
            <td>{{ b.skipped_stale }}</td>
            <td>{{ b.blocked_entities_after }}</td>
            <td class="mono">{{ b.created_at }}</td>
          </tr>
          <tr v-if="!batches.length"><td colspan="11" class="muted">还没有批次，先跑一趟。</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
