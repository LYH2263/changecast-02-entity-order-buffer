<script setup>
import { onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'

const summary = ref([])
const history = ref([])
const showHistory = ref(false)
const lastAction = ref(null)
const err = ref('')
const busy = ref('')

const RESULT_LABEL = {
  open: '等待缺口',
  held: '阻塞滞留',
  released: '补齐放出',
  flushed: '清缓冲放出',
  stale: '过期作废',
}

function tagClass(status) {
  if (status === 'open') return 'warn'
  if (status === 'held') return 'danger'
  return 'ok'
}

function waitOf(bufferedAt) {
  const secs = Math.max(0, Math.floor((Date.now() - new Date(bufferedAt).getTime()) / 1000))
  if (secs < 60) return `${secs}s`
  return `${Math.floor(secs / 60)}m${secs % 60}s`
}

async function refresh() {
  summary.value = await getJSON('/api/buffer-summary')
  if (showHistory.value) history.value = await getJSON('/api/ooo-buffer?status=all')
}

async function toggleHistory() {
  showHistory.value = !showHistory.value
  if (showHistory.value) history.value = await getJSON('/api/ooo-buffer?status=all')
}

async function flush(streamId, entityKey) {
  if (!window.confirm(`确认清缓冲并放行实体 ${entityKey}？\n将以缓冲中最小 seq 作为显式允许的新起始 seq，缺口计入批次。`)) return
  err.value = ''
  busy.value = `flush:${entityKey}`
  try {
    const b = await postJSON('/api/ooo-buffer/flush', { stream_id: streamId, entity_key: entityKey })
    lastAction.value = {
      title: `清缓冲放行 ${entityKey} · 批次 ${b.batch_no}`,
      lines: [
        `连续放出 ${b.flushed_upsert + b.flushed_delete} 条（写 ${b.flushed_upsert} / 删 ${b.flushed_delete}），跳过缺口 ${b.gap_skipped} 条`,
        `缓冲余量 ${b.buffer_open_after} · 阻塞实体 ${b.blocked_entities_after}`,
        ...(b.reasons || []),
      ],
    }
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = ''
  }
}

async function runFill(stream) {
  err.value = ''
  busy.value = `fill:${stream.stream_id}`
  try {
    const b = await postJSON('/api/project', { stream_id: stream.stream_id, fixture: stream.fill_fixture, note: '补齐缺口运行' })
    lastAction.value = {
      title: `补齐夹具 ${stream.fill_fixture} · 批次 ${b.batch_no}`,
      lines: [
        `新到事件 ${b.source_events}：应用 ${b.applied_upsert + b.applied_delete} · 入缓 ${b.buffered} · 滞留 ${b.held_blocked} · 过期 ${b.skipped_stale} · 重复 ${b.skipped_duplicate}`,
        `缓冲连续放出 ${b.released_upsert + b.released_delete} 条 · 缓冲余量 ${b.buffer_open_after} · 阻塞实体 ${b.blocked_entities_after}`,
        ...(b.reasons || []),
      ],
    }
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = ''
  }
}

onMounted(async () => {
  try { await refresh() } catch (e) { err.value = String(e.message || e) }
})
</script>

<template>
  <div class="page">
    <h1>缓冲区 · 按实体保序</h1>
    <p class="muted">
      同一实体键上仅当事件 seq 严格等于该实体期望的下一个 seq 才改投影；超前事件进可审计缓冲，不动投影；
      迟到低 seq 直接判过期，绝不回盖高 seq。逾限策略已选定「<b>阻塞该实体</b>」：该实体后续事件滞留，
      直到人工清缓冲或补齐缺口，其他实体照常应用。清缓冲 / 补齐后按实体内 seq 连续放出。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>

    <div class="panel">
      <h3>演示路径（乱序演练流 reorder_lab）</h3>
      <ol class="demo-steps">
        <li>到 <router-link to="/run">投影台</router-link> 跑 <span class="mono">reorder_lab</span>：L-A 超前入缓、同批补齐后放出；L-B 缓冲逾 2 条上限被阻塞；L-D 挂缺口等待；L-E 的迟到低 seq 被拦下。</li>
        <li>回到本页：对 <b>L-D</b> 点「清缓冲并放行」——显式允许新起始 seq，缺口计数，连续放出。</li>
        <li>对 <b>L-B</b> 点「跑补齐夹具」——<span class="mono">reorder_lab_fill.json</span> 带来 seq 2，阻塞实体连续放出并解除阻塞。</li>
        <li>到 <router-link to="/batches">应用批次</router-link> / <router-link to="/events">事件留痕</router-link> / <router-link to="/projections">投影快照</router-link> 互证计数。</li>
      </ol>
    </div>

    <div class="panel" v-if="lastAction">
      <h3>最近操作</h3>
      <p style="margin:0 0 0.4rem"><b>{{ lastAction.title }}</b></p>
      <p v-for="(line, i) in lastAction.lines" :key="i" class="muted" style="margin:0.15rem 0">{{ line }}</p>
    </div>

    <div class="panel" v-for="s in summary" :key="s.stream_id">
      <h3>
        {{ s.title }} <span class="mono" style="font-size:0.8rem">{{ s.code }}</span>
        <span class="tag" style="margin-left:0.5rem">上限 {{ s.config.buffer_max_events }} 条 / {{ s.config.buffer_max_wait_seconds }}s</span>
        <span class="tag">起始 seq {{ s.config.start_seq }}</span>
        <span class="tag danger">策略 {{ s.config.overflow_policy }}</span>
      </h3>
      <p v-if="!s.entities.length" class="muted" style="margin:0">当前无缓冲、无阻塞实体。</p>
      <div v-for="e in s.entities" :key="e.entity_key" class="entity-block">
        <div class="entity-head">
          <span class="mono entity-key">{{ e.entity_key }}</span>
          <span class="tag" :class="e.blocked ? 'danger' : 'warn'">{{ e.blocked ? '已阻塞' : '缓冲中' }}</span>
          <span class="muted">期望下一 seq <b class="mono">{{ e.expected_seq }}</b></span>
          <span v-if="e.gap.length" class="muted">缺口 <b class="mono">{{ e.gap.join(', ') }}</b></span>
          <span v-if="e.blocked_reason" class="tag danger">{{ e.blocked_reason }}</span>
          <span style="flex:1"></span>
          <button
            :disabled="busy === `flush:${e.entity_key}` || !e.entries.length"
            @click="flush(s.stream_id, e.entity_key)"
          >{{ busy === `flush:${e.entity_key}` ? '放行中…' : '清缓冲并放行' }}</button>
        </div>
        <table v-if="e.entries.length">
          <thead>
            <tr><th>event_id</th><th>seq</th><th>op</th><th>状态</th><th>入缓时间</th><th>已等待</th><th>载荷</th></tr>
          </thead>
          <tbody>
            <tr v-for="r in e.entries" :key="r.id">
              <td class="mono">{{ r.event_id }}</td>
              <td class="mono">{{ r.seq }}</td>
              <td class="mono">{{ r.op }}</td>
              <td><span class="tag" :class="tagClass(r.status)">{{ RESULT_LABEL[r.status] || r.status }}</span></td>
              <td class="mono">{{ r.buffered_at }}</td>
              <td class="mono">{{ waitOf(r.buffered_at) }}</td>
              <td class="mono">{{ r.payload == null ? '—' : JSON.stringify(r.payload) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <p v-if="s.entities.length" style="margin:0.6rem 0 0">
        <button :disabled="busy === `fill:${s.stream_id}`" @click="runFill(s)">
          {{ busy === `fill:${s.stream_id}` ? '运行中…' : `跑补齐夹具 ${s.fill_fixture}` }}
        </button>
        <span class="muted" style="margin-left:0.6rem">用 {{ s.fill_fixture }} 再跑一趟本流：缺口事件按序应用并连续放出缓冲。</span>
      </p>
    </div>

    <div class="panel">
      <h3 style="display:inline-block;margin-right:1rem">缓冲留痕</h3>
      <button @click="toggleHistory">{{ showHistory ? '收起' : '展开全部（含已放出/作废）' }}</button>
      <table v-if="showHistory" style="margin-top:0.75rem">
        <thead>
          <tr><th>流</th><th>event_id</th><th>实体键</th><th>seq</th><th>op</th><th>状态</th><th>入缓时间</th><th>放出批次</th></tr>
        </thead>
        <tbody>
          <tr v-for="r in history" :key="r.id">
            <td>{{ r.stream_id }}</td>
            <td class="mono">{{ r.event_id }}</td>
            <td class="mono">{{ r.entity_key }}</td>
            <td class="mono">{{ r.seq }}</td>
            <td class="mono">{{ r.op }}</td>
            <td><span class="tag" :class="tagClass(r.status)">{{ RESULT_LABEL[r.status] || r.status }}</span></td>
            <td class="mono">{{ r.buffered_at }}</td>
            <td class="mono">{{ r.released_batch_id || '—' }}</td>
          </tr>
          <tr v-if="!history.length"><td colspan="8" class="muted">尚无缓冲留痕。</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<style scoped>
.demo-steps { margin: 0; padding-left: 1.2rem; }
.demo-steps li { margin: 0.3rem 0; }
.entity-block {
  border: 1px solid var(--line);
  background: #fff;
  padding: 0.7rem 0.85rem;
  margin: 0.6rem 0;
}
.entity-head {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.6rem;
  margin-bottom: 0.5rem;
}
.entity-key { font-size: 1.02rem; font-weight: 700; }
.entity-block table { font-size: 0.88rem; }
</style>
