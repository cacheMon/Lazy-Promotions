//
//  OptClock, the same as FIFO-Reinsertion or second chance, is a FIFO with
//  which inserts back some objects upon eviction
//
//
//  OptClock.c
//  libCacheSim
//
//  Created by Juncheng on 12/4/18.
//  Copyright © 2018 Juncheng. All rights reserved.
//

#include <assert.h>
#include <stdint.h>
#include <stdio.h>

#include <cstdlib>
#include <iostream>
#include <unordered_map>
#include <unordered_set>

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"

// #define USE_BELADY
#undef USE_BELADY

static const char *DEFAULT_PARAMS = "n-bit-counter=1,iter=2";

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void OptClock_parse_params(cache_t *cache, const char *cache_specific_params);
static void OptClock_free(cache_t *cache);
static bool OptClock_get(cache_t *cache, const request_t *req);
static cache_obj_t *OptClock_find(cache_t *cache, const request_t *req, const bool update_cache);
static cache_obj_t *OptClock_insert(cache_t *cache, const request_t *req);
static cache_obj_t *OptClock_to_evict(cache_t *cache, const request_t *req);
static void OptClock_evict(cache_t *cache, const request_t *req);
static bool OptClock_remove(cache_t *cache, const obj_id_t obj_id);
static void OptClock_reset(cache_t *cache);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ***********************************************************************
struct lifetime_md {
  uint64_t lifetime_freq = 0;
  uint64_t last_promotion = 0;
};
struct OptClock_params_t {
  cache_obj_t *q_head;
  cache_obj_t *q_tail;

  int n_bit_counter;
  int max_freq;
  int decrease_rate;

  int64_t n_obj_rewritten;
  int64_t n_byte_rewritten;

  int64_t miss;
  int64_t vtime;

  int64_t iteration;
  bool active;

  std::unordered_map<obj_id_t, lifetime_md> lifetime_mds = {};
  std::unordered_map<obj_id_t, std::unordered_set<uint64_t>> bad_promotions = {};
};
cache_t *OptClock_init(const common_cache_params_t ccache_params, const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("OptClock", ccache_params, cache_specific_params);
  cache->cache_init = OptClock_init;
  cache->cache_free = OptClock_free;
  cache->get = OptClock_get;
  cache->find = OptClock_find;
  cache->insert = OptClock_insert;
  cache->evict = OptClock_evict;
  cache->remove = OptClock_remove;
  cache->can_insert = cache_can_insert_default;
  cache->get_n_obj = cache_get_n_obj_default;
  cache->get_occupied_byte = cache_get_occupied_byte_default;
  cache->to_evict = OptClock_to_evict;
  cache->reset_cache = OptClock_reset;
  cache->obj_md_size = 0;
  cache->num_stats = 0;
  cache->num_stats2 = 0;
  cache->num_stats3 = 0;
  cache->num_stats4 = 0;
  cache->num_stats5 = 0;

  cache->eviction_params = new OptClock_params_t;
  OptClock_params_t *params = (OptClock_params_t *)cache->eviction_params;
  params->q_head = NULL;
  params->q_tail = NULL;
  params->n_bit_counter = 1;
  params->max_freq = 1;
  params->decrease_rate = 1;

  OptClock_parse_params(cache, DEFAULT_PARAMS);
  if (cache_specific_params != NULL) {
    OptClock_parse_params(cache, cache_specific_params);
  }

  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "OptClock-%d-%ld", params->n_bit_counter, cache->version_num + 1);
  return cache;
}

static void OptClock_free(cache_t *cache) {
  delete (OptClock_params_t *)cache->eviction_params;
  cache_struct_free(cache);
}

static bool OptClock_get(cache_t *cache, const request_t *req) { return cache_get_base(cache, req); }

static cache_obj_t *OptClock_find(cache_t *cache, const request_t *req, const bool update_cache) {
  OptClock_params_t *params = (OptClock_params_t *)cache->eviction_params;
  params->lifetime_mds[req->obj_id].lifetime_freq += 1;
  cache_obj_t *obj = cache_find_base(cache, req, update_cache);
  if (obj != NULL && update_cache) {
    if (obj->opt_clock.freq < params->max_freq) {
      obj->opt_clock.freq += 1;
    }
  }

  return obj;
}
static cache_obj_t *OptClock_insert(cache_t *cache, const request_t *req) {
  OptClock_params_t *params = (OptClock_params_t *)cache->eviction_params;
  cache_obj_t *obj = cache_insert_base(cache, req);
  prepend_obj_to_head(&params->q_head, &params->q_tail, obj);
  obj->opt_clock.freq = 0;
  obj->opt_clock.promotion_time = 0;
  return obj;
}

static cache_obj_t *OptClock_to_evict(cache_t *cache, const request_t *req) {
  // OptClock_params_t *params = (OptClock_params_t *)cache->eviction_params;
  // int n_round = 0;
  // cache_obj_t *obj_to_evict = params->q_tail;
  // while (obj_to_evict->opt_clock.freq - n_round >= 1) {
  //   obj_to_evict = obj_to_evict->queue.prev;
  //   if (obj_to_evict == NULL) {
  //     obj_to_evict = params->q_tail;
  //     n_round += 1;
  //   }
  // }
  //
  // return obj_to_evict;
  assert(false);
}

static void OptClock_force_evict(cache_t *cache) {
  OptClock_params_t *params = (OptClock_params_t *)cache->eviction_params;
  cache_obj_t *obj_to_evict = params->q_tail;
  remove_obj_from_list(&params->q_head, &params->q_tail, obj_to_evict);
  cache_evict_base(cache, obj_to_evict, true);
}

static void OptClock_evict(cache_t *cache, const request_t *req) {
  OptClock_params_t *params = (OptClock_params_t *)cache->eviction_params;
  cache_obj_t *obj_to_evict = params->q_tail;
  while (obj_to_evict->opt_clock.freq >= 1) {
    auto &lifetime_md = params->lifetime_mds[obj_to_evict->obj_id];
    auto &md = obj_to_evict->opt_clock;
    md.promotion_time = lifetime_md.lifetime_freq;
    if (params->bad_promotions[obj_to_evict->obj_id].find(md.promotion_time) !=
        params->bad_promotions[obj_to_evict->obj_id].end()) {
      break;
    }

    obj_to_evict->opt_clock.freq -= params->decrease_rate;
    params->n_obj_rewritten += 1;
    params->n_byte_rewritten += obj_to_evict->obj_size;

    move_obj_to_head(&params->q_head, &params->q_tail, obj_to_evict);
    cache->n_promotion += 1;
    obj_to_evict = params->q_tail;
  }
  // if (obj_to_evict->opt_clock.promotion_time != 0)
  params->bad_promotions[obj_to_evict->obj_id].insert(obj_to_evict->opt_clock.promotion_time);

  remove_obj_from_list(&params->q_head, &params->q_tail, obj_to_evict);
  cache_evict_base(cache, obj_to_evict, true);
}

static void OptClock_remove_obj(cache_t *cache, cache_obj_t *obj) {
  OptClock_params_t *params = (OptClock_params_t *)cache->eviction_params;

  DEBUG_ASSERT(obj != NULL);
  remove_obj_from_list(&params->q_head, &params->q_tail, obj);
  cache_remove_obj_base(cache, obj, true);
}

static bool OptClock_remove(cache_t *cache, const obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }

  OptClock_remove_obj(cache, obj);

  return true;
}

static const char *OptClock_current_params(cache_t *cache, OptClock_params_t *params) {
  static __thread char params_str[128];
  snprintf(params_str, 128, "n-bit-counter=%d,iter=%d\n", params->n_bit_counter, cache->n_iterations);

  return params_str;
}

static void OptClock_parse_params(cache_t *cache, const char *cache_specific_params) {
  OptClock_params_t *params = (OptClock_params_t *)cache->eviction_params;
  char *params_str = strdup(cache_specific_params);
  char *old_params_str = params_str;
  char *end;

  while (params_str != NULL && params_str[0] != '\0') {
    /* different parameters are separated by comma,
     * key and value are separated by = */
    char *key = strsep((char **)&params_str, "=");
    char *value = strsep((char **)&params_str, ",");

    // skip the white space
    while (params_str != NULL && *params_str == ' ') {
      params_str++;
    }

    if (strcasecmp(key, "n-bit-counter") == 0) {
      params->n_bit_counter = (int)strtol(value, &end, 0);
      params->max_freq = (1 << params->n_bit_counter) - 1;
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }
    } else if (strcasecmp(key, "iter") == 0) {
      cache->n_iterations = (int)strtol(value, &end, 0);
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }
    } else if (strcasecmp(key, "print") == 0) {
      printf("current parameters: %s\n", OptClock_current_params(cache, params));
      exit(0);
    } else {
      ERROR("%s does not have parameter %s, example paramters %s\n", cache->cache_name, key,
            OptClock_current_params(cache, params));
      exit(1);
    }
  }
  free(old_params_str);
}
static void OptClock_reset(cache_t *cache) {
  OptClock_params_t *params = static_cast<OptClock_params_t *>(cache->eviction_params);
  std::cout << "iteration: " << cache->version_num << ", size: " << params->bad_promotions.size() << "\n";

  // request_t tmp;
  // while (cache->n_obj > 0) {
  //   cache->evict(cache, &tmp);
  // }
  while (cache->n_obj > 0) {
    OptClock_force_evict(cache);
  }

  cache->n_req = 0;
  cache->n_promotion = 0;
  cache->n_insert = 0;

  params->lifetime_mds.clear();
  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "OptClock-%d-%ld", params->n_bit_counter, ++cache->version_num + 1);
}
