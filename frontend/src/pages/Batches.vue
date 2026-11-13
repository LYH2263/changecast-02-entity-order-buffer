<script setup>
import { onMounted, ref } from 'vue'
import { getJSON } from '../api'
const rows = ref([])
const err = ref('')
onMounted(async () => {
  try { rows.value = await getJSON('/api/batches?limit=100') } catch (e) { err.value = String(e.message || e) }
})
</script>
<template>
  <div class="page">
    <h1>应用批次</h1>
    <p class="muted">
      每趟运行 / 清缓冲落一个批次。互证规则：运行批的源事件数 = 即刻应用 + 入缓 + 滞留 + 过期 + 重复 + 冗余；
      清缓冲批的处理数 = 放出 + 过期。缓冲放出、逾限阻塞只作用在所属实体，不整批卡死。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="panel">
      <table>
        <thead>
          <tr>
            <th>批次</th><th>投影流</th><th>类型</th>
            <th>源事件</th><th>应用</th><th>放出</th><th>入缓</th><th>滞留</th>
            <th>过期</th><th>重复</th><th>缺口</th><th>新增阻塞</th>
            <th>存活/已删</th><th>互证</th><th>时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="b in rows" :key="b.id">
            <td class="mono">{{ b.batch_no }}</td>
            <td>{{ b.stream_title }}</td>
            <td><span class="tag" :class="b.kind === 'flush' ? 'warn' : ''">{{ b.kind === 'flush' ? '清缓冲' : '运行' }}</span></td>
            <td>{{ b.source_events }}</td>
            <td>{{ b.applied_upsert + b.applied_delete }}</td>
            <td>{{ b.released_upsert + b.released_delete + b.flushed_upsert + b.flushed_delete }}</td>
            <td>{{ b.buffered }}</td>
            <td>{{ b.held_blocked }}</td>
            <td>{{ b.skipped_stale }}</td>
            <td>{{ b.skipped_duplicate + b.skipped_redundant }}</td>
            <td>{{ b.gap_skipped }}</td>
            <td>{{ b.overflow_blocks }}</td>
            <td>{{ b.projected_alive }}/{{ b.projected_deleted }}</td>
            <td><span class="tag" :class="b.reconciled ? 'ok' : 'danger'">{{ b.reconciled ? '✓' : '✗' }}</span></td>
            <td class="mono">{{ b.created_at }}</td>
          </tr>
          <tr v-if="!rows.length"><td colspan="15" class="muted">尚无批次。</td></tr>
        </tbody>
      </table>
      <p class="muted" style="margin:0.6rem 0 0">注：逾限策略为「阻塞该实体」，丢弃计数恒为 0；阻塞实体数见各批「阻塞」列与缓冲区页。</p>
    </div>
  </div>
</template>
