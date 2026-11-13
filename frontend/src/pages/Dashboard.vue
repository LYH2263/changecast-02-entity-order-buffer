<script setup>
import { onMounted, ref } from 'vue'
import { getJSON } from '../api'
const stats = ref(null)
const err = ref('')
onMounted(async () => {
  try { stats.value = await getJSON('/api/dashboard') } catch (e) { err.value = String(e.message || e) }
})
</script>
<template>
  <div class="page">
    <h1>总览</h1>
    <p class="muted">变更日志投影：源 CDC 事件按到达顺序过实体保序闸门，缓冲、阻塞与投影计数互证。</p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="panel" v-if="stats">
      <div class="stat-grid">
        <div class="stat"><div class="stat-num">{{ stats.stream_count }}</div><div class="stat-label">投影流</div></div>
        <div class="stat"><div class="stat-num">{{ stats.batch_count }}</div><div class="stat-label">应用批次</div></div>
        <div class="stat"><div class="stat-num">{{ stats.projection_alive }}</div><div class="stat-label">存活投影</div></div>
        <div class="stat"><div class="stat-num">{{ stats.projection_deleted }}</div><div class="stat-label">已删投影</div></div>
        <div class="stat"><div class="stat-num">{{ stats.buffer_open }}</div><div class="stat-label">缓冲等待</div></div>
        <div class="stat"><div class="stat-num">{{ stats.buffer_held }}</div><div class="stat-label">阻塞滞留</div></div>
        <div class="stat"><div class="stat-num">{{ stats.blocked_entities }}</div><div class="stat-label">阻塞实体</div></div>
        <div class="stat"><div class="stat-num">{{ stats.stale_skipped }}</div><div class="stat-label">过期跳过</div></div>
        <div class="stat"><div class="stat-num">{{ stats.overflow_blocks_total }}</div><div class="stat-label">累计逾限阻塞</div></div>
      </div>
      <p style="margin-top:1rem">
        应用序 <span class="tag">{{ stats.apply_order }}</span>（按实体键局部保序）·
        逾限策略 <span class="tag danger">{{ stats.overflow_policy }}</span>（阻塞该实体，不丢弃、不整批卡死）
      </p>
      <p><router-link to="/run">去投影台跑一趟 →</router-link> · <router-link to="/buffer">看按实体分组的缓冲 →</router-link></p>
    </div>
  </div>
</template>
