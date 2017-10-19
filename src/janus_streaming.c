#include "plugin.h"

#include <sys/poll.h>

#include "../debug.h"
#include "../apierror.h"
#include "../config.h"
#include "../mutex.h"
#include "../rtp.h"
#include "../rtcp.h"
#include "../record.h"
#include "../utils.h"
#include "../ip-utils.h"


#define JANUS_STREAMING_VP8  0
#define JANUS_STREAMING_H264 1
#define JANUS_STREAMING_VP9  2

/* Error codes */
#define JANUS_STREAMING_ERROR_NO_MESSAGE         450
#define JANUS_STREAMING_ERROR_INVALID_JSON       451
#define JANUS_STREAMING_ERROR_INVALID_REQUEST    452
#define JANUS_STREAMING_ERROR_MISSING_ELEMENT    453
#define JANUS_STREAMING_ERROR_INVALID_ELEMENT    454
#define JANUS_STREAMING_ERROR_NO_SUCH_MOUNTPOINT 455
#define JANUS_STREAMING_ERROR_CANT_CREATE        456
#define JANUS_STREAMING_ERROR_UNAUTHORIZED       457
#define JANUS_STREAMING_ERROR_CANT_SWITCH        458
#define JANUS_STREAMING_ERROR_CANT_RECORD        459
#define JANUS_STREAMING_ERROR_UNKNOWN_ERROR      470

#define JANUS_STREAMING_VERSION        1
#define JANUS_STREAMING_VERSION_STRING "0.0.1"
#define JANUS_STREAMING_DESCRIPTION    "JANUS Streaming plugin"
#define JANUS_STREAMING_NAME           "JANUS Streaming plugin"
#define JANUS_STREAMING_AUTHOR         "Me"
#define JANUS_STREAMING_PACKAGE        "janus.plugin.streaming"


/* Parameter validation */
static struct janus_json_parameter request_parameters[] = {
    {"request", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};
static struct janus_json_parameter id_parameters[] = {
    {"id", JSON_INTEGER, JANUS_JSON_PARAM_REQUIRED | JANUS_JSON_PARAM_POSITIVE}
};
static struct janus_json_parameter watch_parameters[] = {
    {"id", JSON_INTEGER, JANUS_JSON_PARAM_REQUIRED | JANUS_JSON_PARAM_POSITIVE},
    {"pin", JSON_STRING, 0},
    {"offer_audio", JANUS_JSON_BOOL, 0},
    {"offer_video", JANUS_JSON_BOOL, 0},
    {"offer_data", JANUS_JSON_BOOL, 0}
};
static struct janus_json_parameter adminkey_parameters[] = {
    {"admin_key", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};
static struct janus_json_parameter create_parameters[] = {
    {"type", JSON_STRING, JANUS_JSON_PARAM_REQUIRED},
    {"secret", JSON_STRING, 0},
    {"pin", JSON_STRING, 0},
    {"permanent", JANUS_JSON_BOOL, 0}
};
static struct janus_json_parameter rtp_parameters[] = {
    {"id", JSON_INTEGER, JANUS_JSON_PARAM_POSITIVE},
    {"name", JSON_STRING, 0},
    {"description", JSON_STRING, 0},
    {"is_private", JANUS_JSON_BOOL, 0},
    {"audio", JANUS_JSON_BOOL, 0},
    {"video", JANUS_JSON_BOOL, 0}
};
static struct janus_json_parameter live_parameters[] = {
    {"id", JSON_INTEGER, JANUS_JSON_PARAM_POSITIVE},
    {"name", JSON_STRING, 0},
    {"description", JSON_STRING, 0},
    {"is_private", JANUS_JSON_BOOL, 0},
    {"filename", JSON_STRING, JANUS_JSON_PARAM_REQUIRED},
    {"audio", JANUS_JSON_BOOL, 0},
    {"video", JANUS_JSON_BOOL, 0}
};
static struct janus_json_parameter rtp_audio_parameters[] = {
    {"audiomcast", JSON_STRING, 0},
    {"audioport", JSON_INTEGER, JANUS_JSON_PARAM_REQUIRED | JANUS_JSON_PARAM_POSITIVE},
    {"audiopt", JSON_INTEGER, JANUS_JSON_PARAM_REQUIRED | JANUS_JSON_PARAM_POSITIVE},
    {"audiortpmap", JSON_STRING, JANUS_JSON_PARAM_REQUIRED},
    {"audiofmtp", JSON_STRING, 0},
    {"audioiface", JSON_STRING, 0}
};
static struct janus_json_parameter rtp_video_parameters[] = {
    {"videomcast", JSON_STRING, 0},
    {"videoport", JSON_INTEGER, JANUS_JSON_PARAM_REQUIRED | JANUS_JSON_PARAM_POSITIVE},
    {"videopt", JSON_INTEGER, JANUS_JSON_PARAM_REQUIRED | JANUS_JSON_PARAM_POSITIVE},
    {"videortpmap", JSON_STRING, JANUS_JSON_PARAM_REQUIRED},
    {"videofmtp", JSON_STRING, 0},
    {"videobufferkf", JANUS_JSON_BOOL, 0},
    {"videoiface", JSON_STRING, 0},
    {"videosimulcast", JANUS_JSON_BOOL, 0},
    {"videoport2", JSON_INTEGER, JANUS_JSON_PARAM_POSITIVE},
    {"videoport3", JSON_INTEGER, JANUS_JSON_PARAM_POSITIVE},
};
static struct janus_json_parameter rtp_data_parameters[] = {
    {"dataport", JSON_INTEGER, JANUS_JSON_PARAM_REQUIRED | JANUS_JSON_PARAM_POSITIVE},
    {"databuffermsg", JANUS_JSON_BOOL, 0},
    {"dataiface", JSON_STRING, 0}
};
static struct janus_json_parameter destroy_parameters[] = {
    {"id", JSON_INTEGER, JANUS_JSON_PARAM_REQUIRED | JANUS_JSON_PARAM_POSITIVE},
    {"permanent", JANUS_JSON_BOOL, 0}
};
static struct janus_json_parameter recording_parameters[] = {
    {"id", JSON_INTEGER, JANUS_JSON_PARAM_REQUIRED | JANUS_JSON_PARAM_POSITIVE},
    {"action", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};
static struct janus_json_parameter recording_start_parameters[] = {
    {"audio", JSON_STRING, 0},
    {"video", JSON_STRING, 0},
    {"data", JSON_STRING, 0}
};
static struct janus_json_parameter recording_stop_parameters[] = {
    {"audio", JANUS_JSON_BOOL, 0},
    {"video", JANUS_JSON_BOOL, 0},
    {"data", JANUS_JSON_BOOL, 0}
};
static struct janus_json_parameter simulcast_parameters[] = {
    {"substream", JSON_INTEGER, JANUS_JSON_PARAM_POSITIVE},
    {"temporal", JSON_INTEGER, JANUS_JSON_PARAM_POSITIVE}
};


typedef struct janus_streaming_message {
    janus_plugin_session *handle;
    char *transaction;
    json_t *message;
    json_t *jsep;
} janus_streaming_message;

typedef enum janus_streaming_type {
    janus_streaming_type_none = 0,
    janus_streaming_type_live,
    janus_streaming_type_on_demand,
} janus_streaming_type;

typedef enum janus_streaming_source {
    janus_streaming_source_none = 0,
    janus_streaming_source_file,
    janus_streaming_source_rtp,
} janus_streaming_source;

typedef struct janus_streaming_rtp_keyframe {
    gboolean enabled;
    /* If enabled, we store the packets of the last keyframe, to immediately send them for new viewers */
    GList *latest_keyframe;
    /* This is where we store packets while we're still collecting the whole keyframe */
    GList *temp_keyframe;
    guint32 temp_ts;
    janus_mutex mutex;
} janus_streaming_rtp_keyframe;

typedef struct janus_streaming_codecs {
    gint audio_pt;
    char *audio_rtpmap;
    char *audio_fmtp;
    gint video_codec;
    gint video_pt;
    char *video_rtpmap;
    char *video_fmtp;
} janus_streaming_codecs;

typedef struct janus_streaming_mountpoint {
    guint64 id;
    char *name;
    char *description;
    gboolean is_private;
    char *secret;
    char *pin;
    gboolean enabled;
    gboolean active;
    janus_streaming_type streaming_type;
    janus_streaming_source streaming_source;
    void *source;	/* Can differ according to the source type */
    GDestroyNotify source_destroy;
    janus_streaming_codecs codecs;
    gboolean audio, video, data;
    GList/*<unowned janus_streaming_session>*/ *listeners;
    gint64 destroyed;
    janus_mutex mutex;
} janus_streaming_mountpoint;

typedef struct janus_streaming_rtp_source {
    gint audio_port;
    in_addr_t audio_mcast;
    gint video_port[3];
    in_addr_t video_mcast;
    gint data_port;
    janus_recorder *arc;	/* The Janus recorder instance for this streams's audio, if enabled */
    janus_recorder *vrc;	/* The Janus recorder instance for this streams's video, if enabled */
    janus_recorder *drc;	/* The Janus recorder instance for this streams's data, if enabled */
    janus_mutex rec_mutex;	/* Mutex to protect the recorders from race conditions */
    int audio_fd;
    int video_fd[3];
    int data_fd;
    gboolean simulcast;
    gint64 last_received_audio;
    gint64 last_received_video;
    gint64 last_received_data;
    janus_streaming_rtp_keyframe keyframe;
    gboolean buffermsg;
    void *last_msg;
    janus_mutex buffermsg_mutex;
    janus_network_address audio_iface;
    janus_network_address video_iface;
    janus_network_address data_iface;
} janus_streaming_rtp_source;

typedef struct janus_streaming_session {
    janus_plugin_session *handle;
    janus_streaming_mountpoint *mountpoint;
    gboolean started;
    gboolean paused;
    gboolean audio, video, data;		/* Whether audio, video and/or data must be sent to this listener */
    janus_rtp_switching_context context;
    int substream;			/* Which simulcast substream we should forward, in case the mountpoint is simulcasting */
    int substream_target;	/* As above, but to handle transitions (e.g., wait for keyframe) */
    int templayer;			/* Which simulcast temporal layer we should forward, in case the mountpoint is simulcasting */
    int templayer_target;	/* As above, but to handle transitions (e.g., wait for keyframe) */
    gint64 last_relayed;	/* When we relayed the last packet (used to detect when substreams become unavailable) */
    janus_vp8_simulcast_context simulcast_context;
    gboolean stopping;
    volatile gint hangingup;
    gint64 destroyed;	/* Time at which this session was marked as destroyed */
} janus_streaming_session;

/* Packets we get from outside and relay */
typedef struct janus_streaming_rtp_relay_packet {
    rtp_header *data;
    gint length;
    gboolean is_rtp;	/* This may be a data packet and not RTP */
    gboolean is_video;
    gboolean is_keyframe;
    gboolean simulcast;
    int codec, substream;
    uint32_t timestamp;
    uint16_t seq_number;
} janus_streaming_rtp_relay_packet;


#pragma region prototypes

static void janus_streaming_message_free(janus_streaming_message *msg);

janus_plugin *create(void);

int janus_streaming_init(janus_callbacks *callback, const char *config_path);
void janus_streaming_destroy(void);

int janus_streaming_get_api_compatibility(void);
int janus_streaming_get_version(void);
const char *janus_streaming_get_version_string(void);
const char *janus_streaming_get_description(void);
const char *janus_streaming_get_name(void);
const char *janus_streaming_get_author(void);
const char *janus_streaming_get_package(void);

void janus_streaming_create_session(janus_plugin_session *handle, int *error);
struct janus_plugin_result *janus_streaming_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep);
void janus_streaming_setup_media(janus_plugin_session *handle);
void janus_streaming_incoming_rtp(janus_plugin_session *handle, int video, char *buf, int len);
void janus_streaming_incoming_rtcp(janus_plugin_session *handle, int video, char *buf, int len);
void janus_streaming_hangup_media(janus_plugin_session *handle);
void janus_streaming_destroy_session(janus_plugin_session *handle, int *error);
json_t *janus_streaming_query_session(janus_plugin_session *handle);

// static int janus_streaming_get_fd_port(int fd);
static void *janus_streaming_handler(void *data);

janus_streaming_mountpoint *janus_streaming_create_rtp_source(
    uint64_t id, char *name, char *desc,
    gboolean doaudio, char* amcast, const janus_network_address *aiface, uint16_t aport, uint8_t acodec, char *artpmap, char *afmtp,
    gboolean dovideo, char* vmcast, const janus_network_address *viface, uint16_t vport, uint8_t vcodec, char *vrtpmap, char *vfmtp, gboolean bufferkf,
        gboolean simulcast, uint16_t vport2, uint16_t vport3,
    gboolean dodata, const janus_network_address *diface, uint16_t dport, gboolean buffermsg);

static void *janus_streaming_relay_thread(void *data);
static void janus_streaming_relay_rtp_packet(gpointer data, gpointer user_data);

#pragma endregion


#pragma region static

static volatile gint initialized = 0, stopping = 0;
static gboolean notify_events = FALSE;

static janus_config *config;
static janus_callbacks *gateway;
static GThread *handler_thread;
static GHashTable *sessions;
static GAsyncQueue *messages;
static janus_mutex mountpoints_mutex;
static GHashTable *mountpoints;

static janus_mutex sessions_mutex = JANUS_MUTEX_INITIALIZER;
static janus_streaming_message exit_message;

#pragma endregion


static janus_plugin janus_streaming_plugin = JANUS_PLUGIN_INIT (
    .init = janus_streaming_init,
    .destroy = janus_streaming_destroy,

    .get_api_compatibility = janus_streaming_get_api_compatibility,
    .get_version = janus_streaming_get_version,
    .get_version_string = janus_streaming_get_version_string,
    .get_description = janus_streaming_get_description,
    .get_name = janus_streaming_get_name,
    .get_author = janus_streaming_get_author,
    .get_package = janus_streaming_get_package,

    .create_session = janus_streaming_create_session,
    .handle_message = janus_streaming_handle_message,
    .setup_media = janus_streaming_setup_media,
    .incoming_rtp = janus_streaming_incoming_rtp,
    .incoming_rtcp = janus_streaming_incoming_rtcp,
    .hangup_media = janus_streaming_hangup_media,
    .destroy_session = janus_streaming_destroy_session,
    .query_session = janus_streaming_query_session,
);

janus_plugin *create(void) {
    JANUS_LOG(LOG_VERB, "%s created!\n", JANUS_STREAMING_NAME);
    return &janus_streaming_plugin;
}


#pragma region meta

int janus_streaming_get_api_compatibility(void) {
    return JANUS_PLUGIN_API_VERSION;
}

int janus_streaming_get_version(void) {
    return JANUS_STREAMING_VERSION;
}

const char *janus_streaming_get_version_string(void) {
    return JANUS_STREAMING_VERSION_STRING;
}

const char *janus_streaming_get_description(void) {
    return JANUS_STREAMING_DESCRIPTION;
}

const char *janus_streaming_get_name(void) {
    return JANUS_STREAMING_NAME;
}

const char *janus_streaming_get_author(void) {
    return JANUS_STREAMING_AUTHOR;
}

const char *janus_streaming_get_package(void) {
    return JANUS_STREAMING_PACKAGE;
}

#pragma endregion


#pragma region callbacks

int janus_streaming_init(janus_callbacks *callback, const char *config_path) {
    JANUS_LOG(LOG_VERB, "%s!!!\n", "janus_streaming_init");

    struct ifaddrs *ifas = NULL;
    if(getifaddrs(&ifas) || ifas == NULL) {
        JANUS_LOG(LOG_ERR, "Unable to acquire list of network devices/interfaces; some configurations may not work as expected...\n");
    }

    g_atomic_int_set(&initialized, 1);

    /* Read configuration */
    char filename[255];
    g_snprintf(filename, 255, "%s/%s.cfg", config_path, JANUS_STREAMING_PACKAGE);
    JANUS_LOG(LOG_VERB, "Configuration file: %s\n", filename);
    config = janus_config_parse(filename);
    if(config != NULL)
        janus_config_print(config);

    mountpoints = g_hash_table_new_full(g_int64_hash, g_int64_equal, g_free, NULL);

    GList *cl = janus_config_get_categories(config);
    /* Dirty hack: first comes general category, then gstreamer, then others */
    cl = cl->next;
    janus_config_category *cat = (janus_config_category *)cl->data;

    janus_network_address video_iface, audio_iface, data_iface;
    /* RTP live source (e.g., from gstreamer/ffmpeg/vlc/etc.) */
    janus_config_item *id = janus_config_get_item(cat, "id");
    janus_config_item *desc = janus_config_get_item(cat, "description");
    janus_config_item *priv = janus_config_get_item(cat, "is_private");
    janus_config_item *secret = janus_config_get_item(cat, "secret");
    janus_config_item *pin = janus_config_get_item(cat, "pin");
    janus_config_item *audio = janus_config_get_item(cat, "audio");
    janus_config_item *video = janus_config_get_item(cat, "video");
    janus_config_item *data = janus_config_get_item(cat, "data");
    janus_config_item *diface = janus_config_get_item(cat, "dataiface");
    janus_config_item *amcast = janus_config_get_item(cat, "audiomcast");
    janus_config_item *aiface = janus_config_get_item(cat, "audioiface");
    janus_config_item *aport = janus_config_get_item(cat, "audioport");
    janus_config_item *acodec = janus_config_get_item(cat, "audiopt");
    janus_config_item *artpmap = janus_config_get_item(cat, "audiortpmap");
    janus_config_item *afmtp = janus_config_get_item(cat, "audiofmtp");
    janus_config_item *vmcast = janus_config_get_item(cat, "videomcast");
    janus_config_item *viface = janus_config_get_item(cat, "videoiface");
    janus_config_item *vport = janus_config_get_item(cat, "videoport");
    janus_config_item *vcodec = janus_config_get_item(cat, "videopt");
    janus_config_item *vrtpmap = janus_config_get_item(cat, "videortpmap");
    janus_config_item *vfmtp = janus_config_get_item(cat, "videofmtp");
    janus_config_item *vkf = janus_config_get_item(cat, "videobufferkf");
    janus_config_item *vsc = janus_config_get_item(cat, "videosimulcast");
    janus_config_item *vport2 = janus_config_get_item(cat, "videoport2");
    janus_config_item *vport3 = janus_config_get_item(cat, "videoport3");
    janus_config_item *dport = janus_config_get_item(cat, "dataport");
    janus_config_item *dbm = janus_config_get_item(cat, "databuffermsg");
    gboolean is_private = priv && priv->value && janus_is_true(priv->value);
    gboolean doaudio = audio && audio->value && janus_is_true(audio->value);
    gboolean dovideo = video && video->value && janus_is_true(video->value);
    gboolean dodata = data && data->value && janus_is_true(data->value);
    gboolean bufferkf = video && vkf && vkf->value && janus_is_true(vkf->value);
    gboolean simulcast = video && vsc && vsc->value && janus_is_true(vsc->value);
    if(simulcast && bufferkf) {
        /* FIXME We'll need to take care of this */
        JANUS_LOG(LOG_WARN, "Simulcasting enabled, so disabling buffering of keyframes\n");
        bufferkf = FALSE;
    }
    gboolean buffermsg = data && dbm && dbm->value && janus_is_true(dbm->value);
    if(!doaudio && !dovideo && !dodata) {
        JANUS_LOG(LOG_ERR, "Can't add 'rtp' stream '%s', no audio, video or data have to be streamed...\n", cat->name);
        cl = cl->next;
        // continue;
    }
    if(doaudio &&
            (aport == NULL || aport->value == NULL || atoi(aport->value) == 0 ||
            acodec == NULL || acodec->value == NULL ||
            artpmap == NULL || artpmap->value == NULL)) {
        JANUS_LOG(LOG_ERR, "Can't add 'rtp' stream '%s', missing mandatory information for audio...\n", cat->name);
        cl = cl->next;
        // continue;
    }
    if(doaudio && aiface) {
        if(!ifas) {
            JANUS_LOG(LOG_ERR, "Skipping 'rtp' stream '%s', it relies on network configuration but network device information is unavailable...\n", cat->name);
            cl = cl->next;
            // continue;
        }
        if(janus_network_lookup_interface(ifas, aiface->value, &audio_iface) != 0) {
            JANUS_LOG(LOG_ERR, "Can't add 'rtp' stream '%s', invalid network interface configuration for audio...\n", cat->name);
            cl = cl->next;
            // continue;
        }
    }
    if(dovideo &&
            (vport == NULL || vport->value == NULL || atoi(vport->value) == 0 ||
            vcodec == NULL || vcodec->value == NULL ||
            vrtpmap == NULL || vrtpmap->value == NULL)) {
        JANUS_LOG(LOG_ERR, "Can't add 'rtp' stream '%s', missing mandatory information for video...\n", cat->name);
        cl = cl->next;
        // continue;
    }
    if(dodata && (dport == NULL || dport->value == NULL || atoi(dport->value) == 0)) {
        JANUS_LOG(LOG_ERR, "Can't add 'rtp' stream '%s', missing mandatory information for data...\n", cat->name);
        cl = cl->next;
        // continue;
    }
    if(dodata && diface) {
        if(!ifas) {
            JANUS_LOG(LOG_ERR, "Skipping 'rtp' stream '%s', it relies on network configuration but network device information is unavailable...\n", cat->name);
            cl = cl->next;
            // continue;
        }
        if(janus_network_lookup_interface(ifas, diface->value, &data_iface) != 0) {
            JANUS_LOG(LOG_ERR, "Can't add 'rtp' stream '%s', invalid network interface configuration for data...\n", cat->name);
            cl = cl->next;
            // continue;
        }
    }
    if(dovideo && viface) {
        if(!ifas) {
            JANUS_LOG(LOG_ERR, "Skipping 'rtp' stream '%s', it relies on network configuration but network device information is unavailable...\n", cat->name);
            cl = cl->next;
            // continue;
        }
        if(janus_network_lookup_interface(ifas, viface->value, &video_iface) != 0) {
            JANUS_LOG(LOG_ERR, "Can't add 'rtp' stream '%s', invalid network interface configuration for video...\n", cat->name);
            cl = cl->next;
            // continue;
        }
    }
    // if(id == NULL || id->value == NULL) {
    //     JANUS_LOG(LOG_VERB, "Missing id for stream '%s', will generate a random one...\n", cat->name);
    // } else {
    //     janus_mutex_lock(&mountpoints_mutex);
    //     guint64 mpid = g_ascii_strtoull(id->value, 0, 10);
    //     janus_streaming_mountpoint *mp = g_hash_table_lookup(mountpoints, &mpid);
    //     janus_mutex_unlock(&mountpoints_mutex);
    //     if(mp != NULL) {
    //         JANUS_LOG(LOG_ERR, "A stream with the provided ID %s already exists, skipping '%s'\n", id->value, cat->name);
    //         cl = cl->next;
    //         continue;
    //     }
    // }
    JANUS_LOG(LOG_VERB, "Audio %s, Video %s, Data %s\n",
        doaudio ? "enabled" : "NOT enabled",
        dovideo ? "enabled" : "NOT enabled",
        dodata ? "enabled" : "NOT enabled");
    janus_streaming_mountpoint *mp = NULL;

    if((mp = janus_streaming_create_rtp_source(
            (id && id->value) ? g_ascii_strtoull(id->value, 0, 10) : 0,
            (char *)cat->name,
            desc ? (char *)desc->value : NULL,
            doaudio,
            amcast ? (char *)amcast->value : NULL,
            doaudio && aiface && aiface->value ? &audio_iface : NULL,
            (aport && aport->value) ? atoi(aport->value) : 0,
            (acodec && acodec->value) ? atoi(acodec->value) : 0,
            artpmap ? (char *)artpmap->value : NULL,
            afmtp ? (char *)afmtp->value : NULL,
            dovideo,
            vmcast ? (char *)vmcast->value : NULL,
            dovideo && viface && viface->value ? &video_iface : NULL,
            (vport && vport->value) ? atoi(vport->value) : 0,
            (vcodec && vcodec->value) ? atoi(vcodec->value) : 0,
            vrtpmap ? (char *)vrtpmap->value : NULL,
            vfmtp ? (char *)vfmtp->value : NULL,
            bufferkf,
            simulcast,
            (vport2 && vport2->value) ? atoi(vport2->value) : 0,
            (vport3 && vport3->value) ? atoi(vport3->value) : 0,
            dodata,
            dodata && diface && diface->value ? &data_iface : NULL,
            (dport && dport->value) ? atoi(dport->value) : 0,
            buffermsg)) == NULL) {
        JANUS_LOG(LOG_ERR, "Error creating 'rtp' stream '%s'...\n", cat->name);
        cl = cl->next;
        // continue;
    }
    mp->is_private = is_private;
    if(secret && secret->value)
        mp->secret = g_strdup(secret->value);
    if(pin && pin->value)
        mp->pin = g_strdup(pin->value);

    // =========== //

    sessions = g_hash_table_new(NULL, NULL);
    messages = g_async_queue_new_full((GDestroyNotify) janus_streaming_message_free);

    gateway = callback;

    /* TODO: Start the sessions watchdog */

    /* Launch the thread that will handle incoming messages */
    GError *error = NULL;
    handler_thread = g_thread_try_new("streaming handler", janus_streaming_handler, NULL, &error);
    if(error != NULL) {
        JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the Streaming handler thread...\n", error->code, error->message ? error->message : "??");
        return -1;
    }

    JANUS_LOG(LOG_INFO, "%s initialized!\n", JANUS_STREAMING_NAME);
    return 0;
}

void janus_streaming_destroy(void) {
    JANUS_LOG(LOG_VERB, "%s!!!\n", "janus_streaming_destroy");
}

void janus_streaming_create_session(janus_plugin_session *handle, int *error) {
    JANUS_LOG(LOG_VERB, "%s!!!\n", "janus_streaming_create_session");

    janus_streaming_session *session = g_malloc0(sizeof(janus_streaming_session));
    session->handle = handle;
    session->mountpoint = NULL;	/* This will happen later */
    session->started = FALSE;	/* This will happen later */
    session->paused = FALSE;
    session->destroyed = 0;

    handle->plugin_handle = session;

    // janus_mutex_lock(&sessions_mutex);
    // g_hash_table_insert(sessions, handle, session);
    // janus_mutex_unlock(&sessions_mutex);

    // TODO: insert into sessions hash table
}

struct janus_plugin_result *janus_streaming_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep) {
    JANUS_LOG(LOG_VERB, "%s!!!\n", "janus_streaming_handle_message");

    // TODO: validations

    json_t *request = json_object_get(message, "request");
    const char *request_text = json_string_value(request);

    JANUS_LOG(LOG_VERB, "Handle message: %s\n", request_text);

    if (!strcasecmp(request_text, "watch") || !strcasecmp(request_text, "start")) {
        /* These messages are handled asynchronously */

        janus_streaming_message *msg = g_malloc0(sizeof(janus_streaming_message));
        msg->handle = handle;
        msg->transaction = transaction;
        msg->message = message;
        msg->jsep = jsep;

        JANUS_LOG(LOG_VERB, "Pushing to messages queue\n");
        g_async_queue_push(messages, msg);
    }

    return janus_plugin_result_new(JANUS_PLUGIN_OK_WAIT, NULL, NULL);
}

void janus_streaming_setup_media(janus_plugin_session *handle) {
    JANUS_LOG(LOG_VERB, "%s!!!\n", "janus_streaming_setup_media");
    JANUS_LOG(LOG_INFO, "WebRTC media is now available\n");

    if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
        return;
    janus_streaming_session *session = (janus_streaming_session *)handle->plugin_handle;
    if(!session) {
        JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
        return;
    }
    if(session->destroyed)
        return;
    g_atomic_int_set(&session->hangingup, 0);

    /* We only start streaming towards this user when we get this event */
    janus_rtp_switching_context_reset(&session->context);

    /* If this is related to a live RTP mountpoint, any keyframe we can shoot already? */
    // janus_streaming_mountpoint *mountpoint = session->mountpoint;
    // if(mountpoint->streaming_source == janus_streaming_source_rtp) {
    // 	janus_streaming_rtp_source *source = mountpoint->source;
    // 	if(source->keyframe.enabled) {
    // 		JANUS_LOG(LOG_HUGE, "Any keyframe to send?\n");
    // 		janus_mutex_lock(&source->keyframe.mutex);
    // 		if(source->keyframe.latest_keyframe != NULL) {
    // 			JANUS_LOG(LOG_HUGE, "Yep! %d packets\n", g_list_length(source->keyframe.latest_keyframe));
    // 			GList *temp = source->keyframe.latest_keyframe;
    // 			while(temp) {
    // 				janus_streaming_relay_rtp_packet(session, temp->data);
    // 				temp = temp->next;
    // 			}
    // 		}
    // 		janus_mutex_unlock(&source->keyframe.mutex);
    // 	}
    // 	if(source->buffermsg) {
    // 		JANUS_LOG(LOG_HUGE, "Any recent datachannel message to send?\n");
    // 		janus_mutex_lock(&source->buffermsg_mutex);
    // 		if(source->last_msg != NULL) {
    // 			JANUS_LOG(LOG_HUGE, "Yep!\n");
    // 			janus_streaming_relay_rtp_packet(session, source->last_msg);
    // 		}
    // 		janus_mutex_unlock(&source->buffermsg_mutex);
    // 	}
    // }

    session->started = TRUE;

    /* Prepare JSON event */
    json_t *event = json_object();
    json_object_set_new(event, "streaming", json_string("event"));
    json_t *result = json_object();
    json_object_set_new(result, "status", json_string("started"));
    json_object_set_new(event, "result", result);
    int ret = gateway->push_event(handle, &janus_streaming_plugin, NULL, event, NULL);
    JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (%s)\n", ret, janus_get_api_error(ret));
    json_decref(event);
}

void janus_streaming_incoming_rtp(janus_plugin_session *handle, int video, char *buf, int len) {
    JANUS_LOG(LOG_VERB, "%s!!!\n", "janus_streaming_incoming_rtp");
}

void janus_streaming_incoming_rtcp(janus_plugin_session *handle, int video, char *buf, int len) {
    JANUS_LOG(LOG_VERB, "%s!!!\n", "janus_streaming_incoming_rtcp");
}

void janus_streaming_hangup_media(janus_plugin_session *handle) {
    JANUS_LOG(LOG_VERB, "%s!!!\n", "janus_streaming_hangup_medi");
}

void janus_streaming_destroy_session(janus_plugin_session *handle, int *error) {
    JANUS_LOG(LOG_VERB, "%s!!!\n", "janus_streaming_destroy_session");
}

json_t *janus_streaming_query_session(janus_plugin_session *handle) {
    // if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
    //     return NULL;
    // }
    // janus_streaming_session *session = (janus_streaming_session *)handle->plugin_handle;
    // if(!session) {
    //     JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
    //     return NULL;
    // }

    /* What is this user watching, if anything? */
    json_t *info = json_object();
    // janus_streaming_mountpoint *mp = session->mountpoint;
    // json_object_set_new(info, "state", json_string(mp ? "watching" : "idle"));
    // if(mp) {
    //     json_object_set_new(info, "mountpoint_id", json_integer(mp->id));
    //     json_object_set_new(info, "mountpoint_name", mp->name ? json_string(mp->name) : NULL);
    // }
    // json_object_set_new(info, "destroyed", json_integer(session->destroyed));
    json_object_set_new(info, "foo", json_string("bar"));

    return info;
}

#pragma endregion


/* Thread to handle incoming messages */
static void *janus_streaming_handler(void *data) {
    JANUS_LOG(LOG_VERB, "Joining Streaming handler thread\n");
    janus_streaming_message *msg = NULL;
    int error_code = 0;
    char error_cause[512];
    json_t *message = NULL;
    while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping)) {
        msg = g_async_queue_pop(messages);
        if(msg == NULL)
            continue;
        if(msg == &exit_message)
            break;
        if(msg->handle == NULL) {
            janus_streaming_message_free(msg);
            continue;
        }

        janus_streaming_session *session = NULL;
        janus_mutex_lock(&sessions_mutex);
        // if(g_hash_table_lookup(sessions, msg->handle) != NULL ) {
        //     session = (janus_streaming_session *)msg->handle->plugin_handle;
        // }
        session = (janus_streaming_session *)msg->handle->plugin_handle;

        if(!session) {
            JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
            janus_mutex_unlock(&sessions_mutex);
            janus_streaming_message_free(msg);
            continue;
        }
        if(session->destroyed) {
            janus_mutex_unlock(&sessions_mutex);
            janus_streaming_message_free(msg);
            continue;
        }
        janus_mutex_unlock(&sessions_mutex);

        /* Handle request */
        error_code = 0;
        message = NULL;
        if(msg->message == NULL) {
            JANUS_LOG(LOG_ERR, "No message??\n");
            error_code = JANUS_STREAMING_ERROR_NO_MESSAGE;
            g_snprintf(error_cause, 512, "%s", "No message??");
            goto error;
        }
        message = msg->message;


        json_t *request = json_object_get(message, "request");
        const char *request_text = json_string_value(request);
        json_t *result = NULL;
        const char *sdp_type = NULL;
        char *sdp = NULL;

        if(!strcasecmp(request_text, "watch")) {
            json_t *id = json_object_get(message, "id");
            guint64 id_value = json_integer_value(id);

            JANUS_LOG(LOG_VERB, "Watch for id: %lu\n", id_value);

            janus_mutex_lock(&mountpoints_mutex);
            janus_streaming_mountpoint *mp = g_hash_table_lookup(mountpoints, &id_value);
            if(mp == NULL) {
                janus_mutex_unlock(&mountpoints_mutex);
                JANUS_LOG(LOG_VERB, "No such mountpoint/stream %"SCNu64"\n", id_value);
                error_code = JANUS_STREAMING_ERROR_NO_SUCH_MOUNTPOINT;
                g_snprintf(error_cause, 512, "No such mountpoint/stream %"SCNu64"", id_value);
                goto error;
            }
            janus_mutex_unlock(&mountpoints_mutex);

            JANUS_LOG(LOG_VERB, "Request to watch mountpoint/stream %"SCNu64"\n", id_value);
            session->stopping = FALSE;
            session->mountpoint = mp;

            /* Check what we should offer */
            // session->audio = offer_audio ? json_is_true(offer_audio) : TRUE;	/* True by default */
            session->audio = TRUE;	/* True by default */
            if(!mp->audio)
                session->audio = FALSE;	/* ... unless the mountpoint isn't sending any audio */
            // session->video = offer_video ? json_is_true(offer_video) : TRUE;	/* True by default */
            session->video = TRUE;	/* True by default */
            if(!mp->video)
                session->video = FALSE;	/* ... unless the mountpoint isn't sending any video */
            // session->data = offer_data ? json_is_true(offer_data) : TRUE;	/* True by default */
            session->data = TRUE;	/* True by default */
            if(!mp->data)
                session->data = FALSE;	/* ... unless the mountpoint isn't sending any data */
            if((!mp->audio || !session->audio) &&
                    (!mp->video || !session->video) &&
                    (!mp->data || !session->data)) {
                JANUS_LOG(LOG_ERR, "Can't offer an SDP with no audio, video or data for this mountpoint\n");
                error_code = JANUS_STREAMING_ERROR_INVALID_REQUEST;
                g_snprintf(error_cause, 512, "Can't offer an SDP with no audio, video or data for this mountpoint");
                goto error;
            }
            if(mp->streaming_source == janus_streaming_source_rtp) {
                janus_streaming_rtp_source *source = (janus_streaming_rtp_source *)mp->source;
                if(source && source->simulcast) {
                    /* This mountpoint is simulcasting, let's aim high by default */
                    session->substream = -1;
                    session->substream_target = 2;
                    session->templayer = -1;
                    session->templayer_target = 2;
                    janus_vp8_simulcast_context_reset(&session->simulcast_context);
                    /* Unless the request contains a target */
                    json_t *substream = json_object_get(message, "substream");
                    if(substream) {
                        session->substream_target = json_integer_value(substream);
                        JANUS_LOG(LOG_VERB, "Setting video substream to let through (simulcast): %d (was %d)\n",
                            session->substream_target, session->substream);
                    }
                    json_t *temporal = json_object_get(message, "temporal");
                    if(temporal) {
                        session->templayer_target = json_integer_value(temporal);
                        JANUS_LOG(LOG_VERB, "Setting video temporal layer to let through (simulcast): %d (was %d)\n",
                            session->templayer_target, session->templayer);
                    }
                }
            }
            /* Let's prepare an offer now, but let's also check if there0s something we need to skip */
            sdp_type = "offer";	/* We're always going to do the offer ourselves, never answer */
            char sdptemp[2048];
            memset(sdptemp, 0, 2048);
            gchar buffer[512];
            memset(buffer, 0, 512);
            gint64 sessid = janus_get_real_time();
            gint64 version = sessid;	/* FIXME This needs to be increased when it changes, so time should be ok */
            g_snprintf(buffer, 512,
                "v=0\r\no=%s %"SCNu64" %"SCNu64" IN IP4 127.0.0.1\r\n",
                    "-", sessid, version);
            g_strlcat(sdptemp, buffer, 2048);
            g_snprintf(buffer, 512,
                "s=Mountpoint %"SCNu64"\r\n", mp->id);
            g_strlcat(sdptemp, buffer, 2048);
            g_strlcat(sdptemp, "t=0 0\r\n", 2048);
            if(mp->codecs.audio_pt >= 0 && session->audio) {
                /* Add audio line */
                g_snprintf(buffer, 512,
                    "m=audio 1 RTP/SAVPF %d\r\n"
                    "c=IN IP4 1.1.1.1\r\n",
                    mp->codecs.audio_pt);
                g_strlcat(sdptemp, buffer, 2048);
                if(mp->codecs.audio_rtpmap) {
                    g_snprintf(buffer, 512,
                        "a=rtpmap:%d %s\r\n",
                        mp->codecs.audio_pt, mp->codecs.audio_rtpmap);
                    g_strlcat(sdptemp, buffer, 2048);
                }
                if(mp->codecs.audio_fmtp) {
                    g_snprintf(buffer, 512,
                        "a=fmtp:%d %s\r\n",
                        mp->codecs.audio_pt, mp->codecs.audio_fmtp);
                    g_strlcat(sdptemp, buffer, 2048);
                }
                g_strlcat(sdptemp, "a=sendonly\r\n", 2048);
            }
            if(mp->codecs.video_pt >= 0 && session->video) {
                /* Add video line */
                g_snprintf(buffer, 512,
                    "m=video 1 RTP/SAVPF %d\r\n"
                    "c=IN IP4 1.1.1.1\r\n",
                    mp->codecs.video_pt);
                g_strlcat(sdptemp, buffer, 2048);
                if(mp->codecs.video_rtpmap) {
                    g_snprintf(buffer, 512,
                        "a=rtpmap:%d %s\r\n",
                        mp->codecs.video_pt, mp->codecs.video_rtpmap);
                    g_strlcat(sdptemp, buffer, 2048);
                }
                if(mp->codecs.video_fmtp) {
                    g_snprintf(buffer, 512,
                        "a=fmtp:%d %s\r\n",
                        mp->codecs.video_pt, mp->codecs.video_fmtp);
                    g_strlcat(sdptemp, buffer, 2048);
                }
                g_snprintf(buffer, 512,
                    "a=rtcp-fb:%d nack\r\n",
                    mp->codecs.video_pt);
                g_strlcat(sdptemp, buffer, 2048);
                g_snprintf(buffer, 512,
                    "a=rtcp-fb:%d goog-remb\r\n",
                    mp->codecs.video_pt);
                g_strlcat(sdptemp, buffer, 2048);
                g_strlcat(sdptemp, "a=sendonly\r\n", 2048);
            }
            if(mp->data && session->data) {
                /* Add data line */
                g_snprintf(buffer, 512,
                    "m=application 1 DTLS/SCTP 5000\r\n"
                    "c=IN IP4 1.1.1.1\r\n"
                    "a=sctpmap:5000 webrtc-datachannel 16\r\n");
                g_strlcat(sdptemp, buffer, 2048);
            }
            sdp = g_strdup(sdptemp);
            JANUS_LOG(LOG_VERB, "Going to offer this SDP:\n%s\n", sdp);
            result = json_object();
            json_object_set_new(result, "status", json_string("preparing"));
            /* Add the user to the list of watchers and we're done */
            janus_mutex_lock(&mp->mutex);
            mp->listeners = g_list_append(mp->listeners, session);
            janus_mutex_unlock(&mp->mutex);
        } else if (!strcasecmp(request_text, "start")) {
            if(session->mountpoint == NULL) {
                JANUS_LOG(LOG_VERB, "Can't start: no mountpoint set\n");
                error_code = JANUS_STREAMING_ERROR_NO_SUCH_MOUNTPOINT;
                g_snprintf(error_cause, 512, "Can't start: no mountpoint set");
                goto error;
            }
            JANUS_LOG(LOG_VERB, "Starting the streaming\n");
            session->paused = FALSE;
            result = json_object();
            /* We wait for the setup_media event to start: on the other hand, it may have already arrived */
            json_object_set_new(result, "status", json_string(session->started ? "started" : "starting"));
        }

        /* Any SDP to handle? */
        const char *msg_sdp_type = json_string_value(json_object_get(msg->jsep, "type"));
        const char *msg_sdp = json_string_value(json_object_get(msg->jsep, "sdp"));
        if(msg_sdp) {
            JANUS_LOG(LOG_VERB, "This is involving a negotiation (%s) as well (but we really don't care):\n%s\n", msg_sdp_type, msg_sdp);
        }

        /* Prepare JSON event */
        json_t *jsep = json_pack("{ssss}", "type", sdp_type, "sdp", sdp);
        json_t *event = json_object();
        json_object_set_new(event, "streaming", json_string("event"));
        if(result != NULL)
            json_object_set_new(event, "result", result);
        int ret = gateway->push_event(msg->handle, &janus_streaming_plugin, msg->transaction, event, jsep);
        JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (%s)\n", ret, janus_get_api_error(ret));
        g_free(sdp);
        json_decref(event);
        json_decref(jsep);
        janus_streaming_message_free(msg);
        continue;

error:
        {
            /* Prepare JSON error event */
            json_t *event = json_object();
            json_object_set_new(event, "streaming", json_string("event"));
            json_object_set_new(event, "error_code", json_integer(error_code));
            json_object_set_new(event, "error", json_string(error_cause));
            int ret = gateway->push_event(msg->handle, &janus_streaming_plugin, msg->transaction, event, NULL);
            JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (%s)\n", ret, janus_get_api_error(ret));
            json_decref(event);
            janus_streaming_message_free(msg);
        }
    }
    JANUS_LOG(LOG_VERB, "Leaving Streaming handler thread\n");
    return NULL;
}

static void janus_streaming_message_free(janus_streaming_message *msg) {
    // if(!msg || msg == &exit_message)
    //     return;

    // msg->handle = NULL;

    // g_free(msg->transaction);
    // msg->transaction = NULL;
    // if(msg->message)
    //     json_decref(msg->message);
    // msg->message = NULL;
    // if(msg->jsep)
    //     json_decref(msg->jsep);
    // msg->jsep = NULL;

    // g_free(msg);
}


#pragma region helpers

/* Helper to create a listener filedescriptor */
static int janus_streaming_create_fd(int port, in_addr_t mcast, const janus_network_address *iface, const char *listenername, const char *medianame, const char *mountpointname) {
    struct sockaddr_in address;
    janus_network_address_string_buffer address_representation;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if(fd < 0) {
        JANUS_LOG(LOG_ERR, "[%s] Cannot create socket for %s...\n", mountpointname, medianame);
        return -1;
    }
    if(port > 0) {
        if(IN_MULTICAST(ntohl(mcast))) {
#ifdef IP_MULTICAST_ALL
            int mc_all = 0;
            if((setsockopt(fd, IPPROTO_IP, IP_MULTICAST_ALL, (void*) &mc_all, sizeof(mc_all))) < 0) {
                JANUS_LOG(LOG_ERR, "[%s] %s listener setsockopt IP_MULTICAST_ALL failed\n", mountpointname, listenername);
                close(fd);
                return -1;
            }
#endif
            struct ip_mreq mreq;
            memset(&mreq, '\0', sizeof(mreq));
            mreq.imr_multiaddr.s_addr = mcast;
            if(!janus_network_address_is_null(iface)) {
                if(iface->family == AF_INET) {
                    mreq.imr_interface = iface->ipv4;
                    (void) janus_network_address_to_string_buffer(iface, &address_representation); /* This is OK: if we get here iface must be non-NULL */
                    JANUS_LOG(LOG_INFO, "[%s] %s listener using interface address: %s\n", mountpointname, listenername, janus_network_address_string_from_buffer(&address_representation));
                } else {
                    JANUS_LOG(LOG_ERR, "[%s] %s listener: invalid multicast address type (only IPv4 is currently supported by this plugin)\n", mountpointname, listenername);
                    close(fd);
                    return -1;
                }
            } else {
                JANUS_LOG(LOG_WARN, "[%s] No multicast interface for: %s. This may not work as expected if you have multiple network devices (NICs)\n", mountpointname, listenername);
            }
            if(setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) == -1) {
                JANUS_LOG(LOG_ERR, "[%s] %s listener IP_ADD_MEMBERSHIP failed\n", mountpointname, listenername);
                close(fd);
                return -1;
            }
            JANUS_LOG(LOG_INFO, "[%s] %s listener IP_ADD_MEMBERSHIP ok\n", mountpointname, listenername);
        }
    }

    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = INADDR_ANY;
    /* If this is multicast, allow a re-use of the same ports (different groups may be used) */
    if(port > 0 && IN_MULTICAST(ntohl(mcast))) {
        int reuse = 1;
        if(setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) == -1) {
            JANUS_LOG(LOG_ERR, "[%s] %s listener setsockopt SO_REUSEPORT failed\n", mountpointname, listenername);
            close(fd);
            return -1;
        }
        address.sin_addr.s_addr = mcast;
    } else {
        if(!IN_MULTICAST(ntohl(mcast)) && !janus_network_address_is_null(iface)) {
            if(iface->family == AF_INET) {
                address.sin_addr = iface->ipv4;
                (void) janus_network_address_to_string_buffer(iface, &address_representation); /* This is OK: if we get here iface must be non-NULL */
                JANUS_LOG(LOG_INFO, "[%s] %s listener restricted to interface address: %s\n", mountpointname, listenername, janus_network_address_string_from_buffer(&address_representation));
            } else {
                JANUS_LOG(LOG_ERR, "[%s] %s listener: invalid address/restriction type (only IPv4 is currently supported by this plugin)\n", mountpointname, listenername);
                close(fd);
                return -1;
            }
        }
    }
    /* Bind to the specified port */
    if(bind(fd, (struct sockaddr *)(&address), sizeof(struct sockaddr)) < 0) {
        JANUS_LOG(LOG_ERR, "[%s] Bind failed for %s (port %d)...\n", mountpointname, medianame, port);
        close(fd);
        return -1;
    }
    return fd;
}

/* Helpers to destroy a streaming mountpoint. */
static void janus_streaming_rtp_source_free(janus_streaming_rtp_source *source) {
    if(source->audio_fd > -1) {
        close(source->audio_fd);
    }
    if(source->video_fd[0] > -1) {
        close(source->video_fd[0]);
    }
    if(source->video_fd[1] > -1) {
        close(source->video_fd[1]);
    }
    if(source->video_fd[2] > -1) {
        close(source->video_fd[2]);
    }
    if(source->data_fd > -1) {
        close(source->data_fd);
    }
    janus_mutex_lock(&source->keyframe.mutex);
    GList *temp = NULL;
    while(source->keyframe.latest_keyframe) {
        temp = g_list_first(source->keyframe.latest_keyframe);
        source->keyframe.latest_keyframe = g_list_remove_link(source->keyframe.latest_keyframe, temp);
        janus_streaming_rtp_relay_packet *pkt = (janus_streaming_rtp_relay_packet *)temp->data;
        g_free(pkt->data);
        g_free(pkt);
        g_list_free(temp);
    }
    source->keyframe.latest_keyframe = NULL;
    while(source->keyframe.temp_keyframe) {
        temp = g_list_first(source->keyframe.temp_keyframe);
        source->keyframe.temp_keyframe = g_list_remove_link(source->keyframe.temp_keyframe, temp);
        janus_streaming_rtp_relay_packet *pkt = (janus_streaming_rtp_relay_packet *)temp->data;
        g_free(pkt->data);
        g_free(pkt);
        g_list_free(temp);
    }
    source->keyframe.latest_keyframe = NULL;
    janus_mutex_unlock(&source->keyframe.mutex);
    janus_mutex_lock(&source->buffermsg_mutex);
    if(source->last_msg) {
        janus_streaming_rtp_relay_packet *pkt = (janus_streaming_rtp_relay_packet *)source->last_msg;
        g_free(pkt->data);
        g_free(pkt);
        source->last_msg = NULL;
    }
    janus_mutex_unlock(&source->buffermsg_mutex);
    g_free(source);
}

/* Helper to create an RTP live source (e.g., from gstreamer/ffmpeg/vlc/etc.) */
janus_streaming_mountpoint *janus_streaming_create_rtp_source(
        uint64_t id, char *name, char *desc,
        gboolean doaudio, char *amcast, const janus_network_address *aiface, uint16_t aport, uint8_t acodec, char *artpmap, char *afmtp,
        gboolean dovideo, char *vmcast, const janus_network_address *viface, uint16_t vport, uint8_t vcodec, char *vrtpmap, char *vfmtp, gboolean bufferkf,
            gboolean simulcast, uint16_t vport2, uint16_t vport3,
        gboolean dodata, const janus_network_address *diface, uint16_t dport, gboolean buffermsg) {

    JANUS_LOG(LOG_VERB, "janus_streaming_create_rtp_source!!!\n");

    janus_mutex_lock(&mountpoints_mutex);
    if(id == 0) {
        JANUS_LOG(LOG_VERB, "Missing id, will generate a random one...\n");
        while(id == 0) {
            id = janus_random_uint64();
            if(g_hash_table_lookup(mountpoints, &id) != NULL) {
                /* ID already in use, try another one */
                id = 0;
            }
        }
    }
    char tempname[255];
    if(name == NULL) {
        JANUS_LOG(LOG_VERB, "Missing name, will generate a random one...\n");
        memset(tempname, 0, 255);
        g_snprintf(tempname, 255, "%"SCNu64, id);
    }
    if(!doaudio && !dovideo && !dodata) {
        JANUS_LOG(LOG_ERR, "Can't add 'rtp' stream, no audio, video or data have to be streamed...\n");
        janus_mutex_unlock(&mountpoints_mutex);
        return NULL;
    }
    if(doaudio && (artpmap == NULL)) {
        JANUS_LOG(LOG_ERR, "Can't add 'rtp' stream, missing mandatory information for audio...\n");
        janus_mutex_unlock(&mountpoints_mutex);
        return NULL;
    }
    if(dovideo && (vcodec == 0 || vrtpmap == NULL)) {
        JANUS_LOG(LOG_ERR, "Can't add 'rtp' stream, missing mandatory information for video...\n");
        janus_mutex_unlock(&mountpoints_mutex);
        return NULL;
    }
    JANUS_LOG(LOG_VERB, "Audio %s, Video %s, Data %s\n",
        doaudio ? "enabled" : "NOT enabled",
        dovideo ? "enabled" : "NOT enabled",
        dodata ? "enabled" : "NOT enabled");
    /* First of all, let's check if the requested ports are free */
    int audio_fd = -1;
    if(doaudio) {
        audio_fd = janus_streaming_create_fd(aport, amcast ? inet_addr(amcast) : INADDR_ANY, aiface,
            "Audio", "audio", name ? name : tempname);
        if(audio_fd < 0) {
            JANUS_LOG(LOG_ERR, "Can't bind to port %d for audio...\n", aport);
            janus_mutex_unlock(&mountpoints_mutex);
            return NULL;
        }
    }
    int video_fd[3] = {-1, -1, -1};
    if(dovideo) {
        video_fd[0] = janus_streaming_create_fd(vport, vmcast ? inet_addr(vmcast) : INADDR_ANY, viface,
            "Video", "video", name ? name : tempname);
        if(video_fd[0] < 0) {
            JANUS_LOG(LOG_ERR, "Can't bind to port %d for video...\n", vport);
            if(audio_fd > -1)
                close(audio_fd);
            janus_mutex_unlock(&mountpoints_mutex);
            return NULL;
        }
        if(simulcast) {
            if(vport2 > 0) {
                video_fd[1] = janus_streaming_create_fd(vport2, vmcast ? inet_addr(vmcast) : INADDR_ANY, viface,
                    "Video", "video", name ? name : tempname);
                if(video_fd[1] < 0) {
                    JANUS_LOG(LOG_ERR, "Can't bind to port %d for video (2nd port)...\n", vport2);
                    if(audio_fd > -1)
                        close(audio_fd);
                    if(video_fd[0] > -1)
                        close(video_fd[0]);
                    janus_mutex_unlock(&mountpoints_mutex);
                    return NULL;
                }
            }
            if(vport3 > 0) {
                video_fd[2] = janus_streaming_create_fd(vport3, vmcast ? inet_addr(vmcast) : INADDR_ANY, viface,
                    "Video", "video", name ? name : tempname);
                if(video_fd[2] < 0) {
                    JANUS_LOG(LOG_ERR, "Can't bind to port %d for video (3rd port)...\n", vport3);
                    if(audio_fd > -1)
                        close(audio_fd);
                    if(video_fd[0] > -1)
                        close(video_fd[0]);
                    if(video_fd[1] > -1)
                        close(video_fd[1]);
                    janus_mutex_unlock(&mountpoints_mutex);
                    return NULL;
                }
            }
        }
    }
    int data_fd = -1;
    if(dodata) {
        data_fd = janus_streaming_create_fd(dport, INADDR_ANY, diface,
            "Data", "data", name ? name : tempname);
        if(data_fd < 0) {
            JANUS_LOG(LOG_ERR, "Can't bind to port %d for data...\n", dport);
            if(audio_fd > -1)
                close(audio_fd);
            if(video_fd[0] > -1)
                close(video_fd[0]);
            if(video_fd[1] > -1)
                close(video_fd[1]);
            if(video_fd[2] > -1)
                close(video_fd[2]);
            janus_mutex_unlock(&mountpoints_mutex);
            return NULL;
        }
    }
    /* Create the mountpoint */
    janus_network_address nil;
    janus_network_address_nullify(&nil);

    janus_streaming_mountpoint *live_rtp = g_malloc0(sizeof(janus_streaming_mountpoint));
    live_rtp->id = id;
    live_rtp->name = g_strdup(name ? name : tempname);
    char *description = NULL;
    if(desc != NULL)
        description = g_strdup(desc);
    else
        description = g_strdup(name ? name : tempname);
    live_rtp->description = description;
    live_rtp->enabled = TRUE;
    live_rtp->active = FALSE;
    live_rtp->audio = doaudio;
    live_rtp->video = dovideo;
    live_rtp->data = dodata;
    live_rtp->streaming_type = janus_streaming_type_live;
    live_rtp->streaming_source = janus_streaming_source_rtp;
    janus_streaming_rtp_source *live_rtp_source = g_malloc0(sizeof(janus_streaming_rtp_source));
    live_rtp_source->audio_mcast = doaudio ? (amcast ? inet_addr(amcast) : INADDR_ANY) : INADDR_ANY;
    live_rtp_source->audio_iface = doaudio && !janus_network_address_is_null(aiface) ? *aiface : nil;
    live_rtp_source->audio_port = doaudio ? aport : -1;
    live_rtp_source->video_mcast = dovideo ? (vmcast ? inet_addr(vmcast) : INADDR_ANY) : INADDR_ANY;
    live_rtp_source->video_port[0] = dovideo ? vport : -1;
    live_rtp_source->simulcast = dovideo && simulcast;
    live_rtp_source->video_port[1] = live_rtp_source->simulcast ? vport2 : -1;
    live_rtp_source->video_port[2] = live_rtp_source->simulcast ? vport3 : -1;
    live_rtp_source->video_iface = dovideo && !janus_network_address_is_null(viface) ? *viface : nil;
    live_rtp_source->data_port = dodata ? dport : -1;
    live_rtp_source->data_iface = dodata && !janus_network_address_is_null(diface) ? *diface : nil;
    live_rtp_source->arc = NULL;
    live_rtp_source->vrc = NULL;
    live_rtp_source->drc = NULL;
    janus_mutex_init(&live_rtp_source->rec_mutex);
    live_rtp_source->audio_fd = audio_fd;
    live_rtp_source->video_fd[0] = video_fd[0];
    live_rtp_source->video_fd[1] = video_fd[1];
    live_rtp_source->video_fd[2] = video_fd[2];
    live_rtp_source->data_fd = data_fd;
    live_rtp_source->last_received_audio = janus_get_monotonic_time();
    live_rtp_source->last_received_video = janus_get_monotonic_time();
    live_rtp_source->last_received_data = janus_get_monotonic_time();
    live_rtp_source->keyframe.enabled = bufferkf;
    live_rtp_source->keyframe.latest_keyframe = NULL;
    live_rtp_source->keyframe.temp_keyframe = NULL;
    live_rtp_source->keyframe.temp_ts = 0;
    janus_mutex_init(&live_rtp_source->keyframe.mutex);
    live_rtp_source->buffermsg = buffermsg;
    live_rtp_source->last_msg = NULL;
    janus_mutex_init(&live_rtp_source->buffermsg_mutex);
    live_rtp->source = live_rtp_source;
    live_rtp->source_destroy = (GDestroyNotify) janus_streaming_rtp_source_free;
    live_rtp->codecs.audio_pt = doaudio ? acodec : -1;
    live_rtp->codecs.audio_rtpmap = doaudio ? g_strdup(artpmap) : NULL;
    live_rtp->codecs.audio_fmtp = doaudio ? (afmtp ? g_strdup(afmtp) : NULL) : NULL;
    live_rtp->codecs.video_codec = -1;
    if(dovideo) {
        if(strstr(vrtpmap, "vp8") || strstr(vrtpmap, "VP8"))
            live_rtp->codecs.video_codec = JANUS_STREAMING_VP8;
        else if(strstr(vrtpmap, "vp9") || strstr(vrtpmap, "VP9"))
            live_rtp->codecs.video_codec = JANUS_STREAMING_VP9;
        else if(strstr(vrtpmap, "h264") || strstr(vrtpmap, "H264"))
            live_rtp->codecs.video_codec = JANUS_STREAMING_H264;
    }
    live_rtp->codecs.video_pt = dovideo ? vcodec : -1;
    live_rtp->codecs.video_rtpmap = dovideo ? g_strdup(vrtpmap) : NULL;
    live_rtp->codecs.video_fmtp = dovideo ? (vfmtp ? g_strdup(vfmtp) : NULL) : NULL;
    live_rtp->listeners = NULL;
    live_rtp->destroyed = 0;
    janus_mutex_init(&live_rtp->mutex);
    g_hash_table_insert(mountpoints, janus_uint64_dup(live_rtp->id), live_rtp);
    janus_mutex_unlock(&mountpoints_mutex);
    GError *error = NULL;
    char tname[16];
    g_snprintf(tname, sizeof(tname), "mp %"SCNu64, live_rtp->id);
    g_thread_try_new(tname, &janus_streaming_relay_thread, live_rtp, &error);
    if(error != NULL) {
        JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the RTP thread...\n", error->code, error->message ? error->message : "??");
        g_free(live_rtp->name);
        g_free(description);
        g_free(live_rtp_source);
        g_free(live_rtp);
        return NULL;
    }
    return live_rtp;
}

#pragma endregion


/* FIXME Test thread to relay RTP frames coming from gstreamer/ffmpeg/others */
static void *janus_streaming_relay_thread(void *data) {
    JANUS_LOG(LOG_VERB, "Starting streaming relay thread\n");
    janus_streaming_mountpoint *mountpoint = (janus_streaming_mountpoint *)data;
    if(!mountpoint) {
        JANUS_LOG(LOG_ERR, "Invalid mountpoint!\n");
        g_thread_unref(g_thread_self());
        return NULL;
    }
    if(mountpoint->streaming_source != janus_streaming_source_rtp) {
        JANUS_LOG(LOG_ERR, "[%s] Not an RTP source mountpoint!\n", mountpoint->name);
        g_thread_unref(g_thread_self());
        return NULL;
    }
    janus_streaming_rtp_source *source = mountpoint->source;
    if(source == NULL) {
        JANUS_LOG(LOG_ERR, "[%s] Invalid RTP source mountpoint!\n", mountpoint->name);
        g_thread_unref(g_thread_self());
        return NULL;
    }
    int audio_fd = source->audio_fd;
    int video_fd[3] = {source->video_fd[0], source->video_fd[1], source->video_fd[2]};
    int data_fd = source->data_fd;
    char *name = g_strdup(mountpoint->name ? mountpoint->name : "??");
    /* Needed to fix seq and ts */
    uint32_t a_last_ssrc = 0, a_last_ts = 0, a_base_ts = 0, a_base_ts_prev = 0,
            v_last_ssrc[3] = {0, 0, 0}, v_last_ts[3] = {0, 0, 0}, v_base_ts[3] = {0, 0, 0}, v_base_ts_prev[3] = {0, 0, 0};
    uint16_t a_last_seq = 0, a_base_seq = 0, a_base_seq_prev = 0,
            v_last_seq[3] = {0, 0, 0}, v_base_seq[3] = {0, 0, 0}, v_base_seq_prev[3] = {0, 0, 0};
    /* File descriptors */
    socklen_t addrlen;
    struct sockaddr_in remote;
    int resfd = 0, bytes = 0;
    struct pollfd fds[5];
    char buffer[1500];
    memset(buffer, 0, 1500);
    /* Loop */
    int num = 0;
    janus_streaming_rtp_relay_packet packet;
    while(!g_atomic_int_get(&stopping) && !mountpoint->destroyed) {
        /* Prepare poll */
        num = 0;
        if(audio_fd != -1) {
            fds[num].fd = audio_fd;
            fds[num].events = POLLIN;
            fds[num].revents = 0;
            num++;
        }
        if(video_fd[0] != -1) {
            fds[num].fd = video_fd[0];
            fds[num].events = POLLIN;
            fds[num].revents = 0;
            num++;
        }
        if(video_fd[1] != -1) {
            fds[num].fd = video_fd[1];
            fds[num].events = POLLIN;
            fds[num].revents = 0;
            num++;
        }
        if(video_fd[2] != -1) {
            fds[num].fd = video_fd[2];
            fds[num].events = POLLIN;
            fds[num].revents = 0;
            num++;
        }
        if(data_fd != -1) {
            fds[num].fd = data_fd;
            fds[num].events = POLLIN;
            fds[num].revents = 0;
            num++;
        }
        /* Wait for some data */
        resfd = poll(fds, num, 1000);
        JANUS_LOG(LOG_HUGE, "Polling: %d\n", resfd);

        if(resfd < 0) {
            if(errno == EINTR) {
                JANUS_LOG(LOG_HUGE, "[%s] Got an EINTR (%s), ignoring...\n", name, strerror(errno));
                continue;
            }
            JANUS_LOG(LOG_ERR, "[%s] Error polling... %d (%s)\n", name, errno, strerror(errno));
            mountpoint->enabled = FALSE;
            break;
        } else if(resfd == 0) {
            /* No data, keep going */
            continue;
        }
        int i = 0;
        for(i=0; i<num; i++) {
            if(fds[i].revents & (POLLERR | POLLHUP)) {
                /* Socket error? */
                JANUS_LOG(LOG_ERR, "[%s] Error polling: %s... %d (%s)\n", name,
                    fds[i].revents & POLLERR ? "POLLERR" : "POLLHUP", errno, strerror(errno));
                mountpoint->enabled = FALSE;
                break;
            } else if(fds[i].revents & POLLIN) {
                /* Got an RTP or data packet */
                if(audio_fd != -1 && fds[i].fd == audio_fd) {
                    /* Got something audio (RTP) */
                    if(mountpoint->active == FALSE)
                        mountpoint->active = TRUE;
                    source->last_received_audio = janus_get_monotonic_time();
                    addrlen = sizeof(remote);
                    bytes = recvfrom(audio_fd, buffer, 1500, 0, (struct sockaddr*)&remote, &addrlen);
                    if(bytes < 0) {
                        /* Failed to read? */
                        continue;
                    }
                    JANUS_LOG(LOG_VERB, "************************\nGot %d bytes on the audio channel...\n", bytes);
                    /* If paused, ignore this packet */
                    if(!mountpoint->enabled)
                        continue;
                    rtp_header *rtp = (rtp_header *)buffer;
                    JANUS_LOG(LOG_VERB, " ... parsed RTP packet (ssrc=%u, pt=%u, seq=%u, ts=%u)...\n",
                        ntohl(rtp->ssrc), rtp->type, ntohs(rtp->seq_number), ntohl(rtp->timestamp));
                    /* Relay on all sessions */
                    packet.data = rtp;
                    packet.length = bytes;
                    packet.is_rtp = TRUE;
                    packet.is_video = FALSE;
                    packet.is_keyframe = FALSE;
                    /* Do we have a new stream? */
                    if(ntohl(packet.data->ssrc) != a_last_ssrc) {
                        a_last_ssrc = ntohl(packet.data->ssrc);
                        JANUS_LOG(LOG_INFO, "[%s] New audio stream! (ssrc=%u)\n", name, a_last_ssrc);
                        a_base_ts_prev = a_last_ts;
                        a_base_ts = ntohl(packet.data->timestamp);
                        a_base_seq_prev = a_last_seq;
                        a_base_seq = ntohs(packet.data->seq_number);
                    }
                    a_last_ts = (ntohl(packet.data->timestamp)-a_base_ts)+a_base_ts_prev+960;	/* FIXME We're assuming Opus here... */
                    packet.data->timestamp = htonl(a_last_ts);
                    a_last_seq = (ntohs(packet.data->seq_number)-a_base_seq)+a_base_seq_prev+1;
                    packet.data->seq_number = htons(a_last_seq);
                    JANUS_LOG(LOG_VERB, " ... updated RTP packet (ssrc=%u, pt=%u, seq=%u, ts=%u)...\n",
                        ntohl(rtp->ssrc), rtp->type, ntohs(rtp->seq_number), ntohl(rtp->timestamp));
                    packet.data->type = mountpoint->codecs.audio_pt;
                    /* Is there a recorder? */
                    janus_recorder_save_frame(source->arc, buffer, bytes);
                    /* Backup the actual timestamp and sequence number set by the restreamer, in case switching is involved */
                    packet.timestamp = ntohl(packet.data->timestamp);
                    packet.seq_number = ntohs(packet.data->seq_number);
                    /* Go! */
                    janus_mutex_lock(&mountpoint->mutex);
                    g_list_foreach(mountpoint->listeners, janus_streaming_relay_rtp_packet, &packet);
                    janus_mutex_unlock(&mountpoint->mutex);
                    continue;
                } else if((video_fd[0] != -1 && fds[i].fd == video_fd[0]) ||
                        (video_fd[1] != -1 && fds[i].fd == video_fd[1]) ||
                        (video_fd[2] != -1 && fds[i].fd == video_fd[2])) {
                    /* Got something video (RTP) */
                    int index = -1;
                    if(fds[i].fd == video_fd[0])
                        index = 0;
                    else if(fds[i].fd == video_fd[1])
                        index = 1;
                    else if(fds[i].fd == video_fd[2])
                        index = 2;
                    if(mountpoint->active == FALSE)
                        mountpoint->active = TRUE;
                    source->last_received_video = janus_get_monotonic_time();
                    addrlen = sizeof(remote);
                    bytes = recvfrom(fds[i].fd, buffer, 1500, 0, (struct sockaddr*)&remote, &addrlen);
                    if(bytes < 0) {
                        /* Failed to read? */
                        continue;
                    }
                    JANUS_LOG(LOG_HUGE, "************************\nGot %d bytes on the video channel...\n", bytes);
                    rtp_header *rtp = (rtp_header *)buffer;

                    /* First of all, let's check if this is (part of) a keyframe that we may need to save it for future reference */
                    if(source->keyframe.enabled) {
                        if(source->keyframe.temp_ts > 0 && ntohl(rtp->timestamp) != source->keyframe.temp_ts) {
                            /* We received the last part of the keyframe, get rid of the old one and use this from now on */
                            JANUS_LOG(LOG_HUGE, "[%s] ... ... last part of keyframe received! ts=%"SCNu32", %d packets\n",
                                name, source->keyframe.temp_ts, g_list_length(source->keyframe.temp_keyframe));
                            source->keyframe.temp_ts = 0;
                            janus_mutex_lock(&source->keyframe.mutex);
                            GList *temp = NULL;
                            while(source->keyframe.latest_keyframe) {
                                temp = g_list_first(source->keyframe.latest_keyframe);
                                source->keyframe.latest_keyframe = g_list_remove_link(source->keyframe.latest_keyframe, temp);
                                janus_streaming_rtp_relay_packet *pkt = (janus_streaming_rtp_relay_packet *)temp->data;
                                g_free(pkt->data);
                                g_free(pkt);
                                g_list_free(temp);
                            }
                            source->keyframe.latest_keyframe = source->keyframe.temp_keyframe;
                            source->keyframe.temp_keyframe = NULL;
                            janus_mutex_unlock(&source->keyframe.mutex);
                        } else if(ntohl(rtp->timestamp) == source->keyframe.temp_ts) {
                            /* Part of the keyframe we're currently saving, store */
                            janus_mutex_lock(&source->keyframe.mutex);
                            JANUS_LOG(LOG_HUGE, "[%s] ... other part of keyframe received! ts=%"SCNu32"\n", name, source->keyframe.temp_ts);
                            janus_streaming_rtp_relay_packet *pkt = g_malloc0(sizeof(janus_streaming_rtp_relay_packet));
                            pkt->data = g_malloc0(bytes);
                            memcpy(pkt->data, buffer, bytes);
                            pkt->data->ssrc = htons(1);
                            pkt->data->type = mountpoint->codecs.video_pt;
                            packet.is_rtp = TRUE;
                            packet.is_video = TRUE;
                            packet.is_keyframe = TRUE;
                            pkt->length = bytes;
                            pkt->timestamp = source->keyframe.temp_ts;
                            pkt->seq_number = ntohs(rtp->seq_number);
                            source->keyframe.temp_keyframe = g_list_append(source->keyframe.temp_keyframe, pkt);
                            janus_mutex_unlock(&source->keyframe.mutex);
                        } else {
                            gboolean kf = FALSE;
                            /* Parse RTP header first */
                            rtp_header *header = (rtp_header *)buffer;
                            guint32 timestamp = ntohl(header->timestamp);
                            guint16 seq = ntohs(header->seq_number);
                            JANUS_LOG(LOG_HUGE, "Checking if packet (size=%d, seq=%"SCNu16", ts=%"SCNu32") is a key frame...\n",
                                bytes, seq, timestamp);
                            int plen = 0;
                            char *payload = janus_rtp_payload(buffer, bytes, &plen);
                            if(payload) {
                                switch(mountpoint->codecs.video_codec) {
                                    case JANUS_STREAMING_VP8:
                                        kf = janus_vp8_is_keyframe(payload, plen);
                                        break;
                                    case JANUS_STREAMING_VP9:
                                        kf = janus_vp9_is_keyframe(payload, plen);
                                        break;
                                    case JANUS_STREAMING_H264:
                                        kf = janus_h264_is_keyframe(payload, plen);
                                        break;
                                    default:
                                        break;
                                }
                                if(kf) {
                                    /* New keyframe, start saving it */
                                    source->keyframe.temp_ts = ntohl(rtp->timestamp);
                                    JANUS_LOG(LOG_HUGE, "[%s] New keyframe received! ts=%"SCNu32"\n", name, source->keyframe.temp_ts);
                                    janus_mutex_lock(&source->keyframe.mutex);
                                    janus_streaming_rtp_relay_packet *pkt = g_malloc0(sizeof(janus_streaming_rtp_relay_packet));
                                    pkt->data = g_malloc0(bytes);
                                    memcpy(pkt->data, buffer, bytes);
                                    pkt->data->ssrc = htons(1);
                                    pkt->data->type = mountpoint->codecs.video_pt;
                                    packet.is_rtp = TRUE;
                                    packet.is_video = TRUE;
                                    packet.is_keyframe = TRUE;
                                    pkt->length = bytes;
                                    pkt->timestamp = source->keyframe.temp_ts;
                                    pkt->seq_number = ntohs(rtp->seq_number);
                                    source->keyframe.temp_keyframe = g_list_append(source->keyframe.temp_keyframe, pkt);
                                    janus_mutex_unlock(&source->keyframe.mutex);
                                }
                            }
                        }
                    }
                    /* If paused, ignore this packet */
                    if(!mountpoint->enabled)
                        continue;
                    JANUS_LOG(LOG_HUGE, " ... parsed RTP packet (ssrc=%u, pt=%u, seq=%u, ts=%u)...\n",
                        ntohl(rtp->ssrc), rtp->type, ntohs(rtp->seq_number), ntohl(rtp->timestamp));
                    /* Relay on all sessions */
                    packet.data = rtp;
                    packet.length = bytes;
                    packet.is_rtp = TRUE;
                    packet.is_video = TRUE;
                    packet.is_keyframe = FALSE;
                    packet.simulcast = source->simulcast;
                    packet.substream = index;
                    packet.codec = mountpoint->codecs.video_codec;
                    /* Do we have a new stream? */
                    if(ntohl(packet.data->ssrc) != v_last_ssrc[index]) {
                        v_last_ssrc[index] = ntohl(packet.data->ssrc);
                        JANUS_LOG(LOG_INFO, "[%s] New video stream! (ssrc=%u, index %d)\n", name, v_last_ssrc[index], index);
                        v_base_ts_prev[index] = v_last_ts[index];
                        v_base_ts[index] = ntohl(packet.data->timestamp);
                        v_base_seq_prev[index] = v_last_seq[index];
                        v_base_seq[index] = ntohs(packet.data->seq_number);
                    }
                    v_last_ts[index] = (ntohl(packet.data->timestamp)-v_base_ts[index])+v_base_ts_prev[index]+4500;	/* FIXME We're assuming 15fps here... */
                    packet.data->timestamp = htonl(v_last_ts[index]);
                    v_last_seq[index] = (ntohs(packet.data->seq_number)-v_base_seq[index])+v_base_seq_prev[index]+1;
                    packet.data->seq_number = htons(v_last_seq[index]);
                    JANUS_LOG(LOG_HUGE, " ... updated RTP packet (ssrc=%u, pt=%u, seq=%u, ts=%u)...\n",
                        ntohl(rtp->ssrc), rtp->type, ntohs(rtp->seq_number), ntohl(rtp->timestamp));
                    packet.data->type = mountpoint->codecs.video_pt;
                    /* Is there a recorder? (FIXME notice we only record the first substream, if simulcasting) */
                    if(index == 0)
                        janus_recorder_save_frame(source->vrc, buffer, bytes);
                    /* Backup the actual timestamp and sequence number set by the restreamer, in case switching is involved */
                    packet.timestamp = ntohl(packet.data->timestamp);
                    packet.seq_number = ntohs(packet.data->seq_number);
                    /* Go! */
                    janus_mutex_lock(&mountpoint->mutex);

                    JANUS_LOG(LOG_HUGE, "Listeners count: %d\n", g_list_length(mountpoint->listeners));

                    g_list_foreach(mountpoint->listeners, janus_streaming_relay_rtp_packet, &packet);
                    janus_mutex_unlock(&mountpoint->mutex);
                    continue;
                } else if(data_fd != -1 && fds[i].fd == data_fd) {
                    /* Got something data (text) */
                    if(mountpoint->active == FALSE)
                        mountpoint->active = TRUE;
                    source->last_received_data = janus_get_monotonic_time();
                    addrlen = sizeof(remote);
                    bytes = recvfrom(data_fd, buffer, 1500, 0, (struct sockaddr*)&remote, &addrlen);
                    if(bytes < 0) {
                        /* Failed to read? */
                        continue;
                    }
                    /* Get a string out of the data */
                    char *text = g_malloc0(bytes+1);
                    memcpy(text, buffer, bytes);
                    *(text+bytes) = '\0';
                    /* Relay on all sessions */
                    packet.data = (rtp_header *)text;
                    packet.length = bytes+1;
                    packet.is_rtp = FALSE;
                    /* Is there a recorder? */
                    janus_recorder_save_frame(source->drc, text, strlen(text));
                    /* Are we keeping track of the last message being relayed? */
                    if(source->buffermsg) {
                        janus_mutex_lock(&source->buffermsg_mutex);
                        janus_streaming_rtp_relay_packet *pkt = g_malloc0(sizeof(janus_streaming_rtp_relay_packet));
                        pkt->data = g_malloc0(bytes+1);
                        memcpy(pkt->data, text, bytes+1);
                        packet.is_rtp = FALSE;
                        pkt->length = bytes+1;
                        janus_mutex_unlock(&source->buffermsg_mutex);
                    }
                    /* Go! */
                    janus_mutex_lock(&mountpoint->mutex);
                    g_list_foreach(mountpoint->listeners, janus_streaming_relay_rtp_packet, &packet);
                    janus_mutex_unlock(&mountpoint->mutex);
                    packet.data = NULL;
                    g_free(text);
                    continue;
                }
            }
        }
    }

    /* Notify users this mountpoint is done */
    janus_mutex_lock(&mountpoint->mutex);
    GList *viewer = g_list_first(mountpoint->listeners);
    /* Prepare JSON event */
    json_t *event = json_object();
    json_object_set_new(event, "streaming", json_string("event"));
    json_t *result = json_object();
    json_object_set_new(result, "status", json_string("stopped"));
    json_object_set_new(event, "result", result);
    while(viewer) {
        janus_streaming_session *session = (janus_streaming_session *)viewer->data;
        if(session != NULL) {
            session->stopping = TRUE;
            session->started = FALSE;
            session->paused = FALSE;
            session->mountpoint = NULL;
            /* Tell the core to tear down the PeerConnection, hangup_media will do the rest */
            gateway->push_event(session->handle, &janus_streaming_plugin, NULL, event, NULL);
            gateway->close_pc(session->handle);
        }
        mountpoint->listeners = g_list_remove_all(mountpoint->listeners, session);
        viewer = g_list_first(mountpoint->listeners);
    }
    json_decref(event);
    janus_mutex_unlock(&mountpoint->mutex);

    JANUS_LOG(LOG_VERB, "[%s] Leaving streaming relay thread\n", name);
    g_free(name);
    g_thread_unref(g_thread_self());
    return NULL;
}

static void janus_streaming_relay_rtp_packet(gpointer data, gpointer user_data) {
    janus_streaming_rtp_relay_packet *packet = (janus_streaming_rtp_relay_packet *)user_data;
    if(!packet || !packet->data || packet->length < 1) {
        JANUS_LOG(LOG_ERR, "Invalid packet...\n");
        return;
    }
    janus_streaming_session *session = (janus_streaming_session *)data;
    if(!session || !session->handle) {
        JANUS_LOG(LOG_ERR, "Invalid session...\n");
        return;
    }
    if(!packet->is_keyframe && (!session->started || session->paused)) {
        JANUS_LOG(LOG_ERR, "Streaming not started yet for this session...\n");
        return;
    }

    if(packet->is_rtp) {
        /* Make sure there hasn't been a publisher switch by checking the SSRC */
        if(packet->is_video) {
            if(!session->video)
                return;
            if(packet->simulcast) {
                /* Handle simulcast: don't relay if it's not the substream we wanted to handle */
                int plen = 0;
                char *payload = janus_rtp_payload((char *)packet->data, packet->length, &plen);
                if(payload == NULL)
                    return;
                gboolean switched = FALSE;
                if(session->substream != session->substream_target) {
                    /* There has been a change: let's wait for a keyframe on the target */
                    int step = (session->substream < 1 && session->substream_target == 2);
                    if(packet->substream == session->substream_target || (step && packet->substream == step)) {
                        if(janus_vp8_is_keyframe(payload, plen)) {
                            JANUS_LOG(LOG_VERB, "Received keyframe on substream %d, switching (was %d)\n",
                                packet->substream, session->substream);
                            session->substream = packet->substream;
                            switched = TRUE;
                            /* Notify the viewer */
                            json_t *event = json_object();
                            json_object_set_new(event, "streaming", json_string("event"));
                            json_t *result = json_object();
                            json_object_set_new(result, "substream", json_integer(session->substream));
                            json_object_set_new(event, "result", result);
                            gateway->push_event(session->handle, &janus_streaming_plugin, NULL, event, NULL);
                            json_decref(event);
                        // } else {
                        //     JANUS_LOG(LOG_WARN, "Not a keyframe on SSRC %"SCNu32" yet, waiting before switching\n", ssrc);
                        }
                    }
                }
                /* If we haven't received our desired substream yet, let's drop temporarily */
                if(session->last_relayed == 0) {
                    /* Let's start slow */
                    session->last_relayed = janus_get_monotonic_time();
                } else {
                    /* Check if 250ms went by with no packet relayed */
                    gint64 now = janus_get_monotonic_time();
                    if(now-session->last_relayed >= 250000) {
                        session->last_relayed = now;
                        int substream = session->substream-1;
                        if(substream < 0)
                            substream = 0;
                        if(session->substream != substream) {
                            JANUS_LOG(LOG_WARN, "No packet received on substream %d for a while, falling back to %d\n",
                                session->substream, substream);
                            session->substream = substream;
                            /* Notify the viewer */
                            json_t *event = json_object();
                            json_object_set_new(event, "streaming", json_string("event"));
                            json_t *result = json_object();
                            json_object_set_new(result, "substream", json_integer(session->substream));
                            json_object_set_new(event, "result", result);
                            gateway->push_event(session->handle, &janus_streaming_plugin, NULL, event, NULL);
                            json_decref(event);
                        }
                    }
                }
                if(packet->substream != session->substream) {
                    JANUS_LOG(LOG_HUGE, "Dropping packet (it's from substream %d, but we're only relaying substream %d now\n",
                        packet->substream, session->substream);
                    return;
                }
                session->last_relayed = janus_get_monotonic_time();
                char vp8pd[6];
                if(packet->codec == JANUS_STREAMING_VP8) {
                    /* Check if there's any temporal scalability to take into account */
                    uint16_t picid = 0;
                    uint8_t tlzi = 0;
                    uint8_t tid = 0;
                    uint8_t ybit = 0;
                    uint8_t keyidx = 0;
                    if(janus_vp8_parse_descriptor(payload, plen, &picid, &tlzi, &tid, &ybit, &keyidx) == 0) {
                        //~ JANUS_LOG(LOG_WARN, "%"SCNu16", %u, %u, %u, %u\n", picid, tlzi, tid, ybit, keyidx);
                        if(session->templayer != session->templayer_target) {
                            /* FIXME We should be smarter in deciding when to switch */
                            session->templayer = session->templayer_target;
                                /* Notify the viewer */
                                json_t *event = json_object();
                                json_object_set_new(event, "streaming", json_string("event"));
                                json_t *result = json_object();
                                json_object_set_new(result, "temporal", json_integer(session->templayer));
                                json_object_set_new(event, "result", result);
                                gateway->push_event(session->handle, &janus_streaming_plugin, NULL, event, NULL);
                                json_decref(event);
                        }
                        if(tid > session->templayer) {
                            JANUS_LOG(LOG_HUGE, "Dropping packet (it's temporal layer %d, but we're capping at %d)\n",
                                tid, session->templayer);
                            /* We increase the base sequence number, or there will be gaps when delivering later */
                            session->context.v_base_seq++;
                            return;
                        }
                    }
                    /* If we got here, update the RTP header and send the packet */
                    janus_rtp_header_update(packet->data, &session->context, TRUE, 4500);
                    memcpy(vp8pd, payload, sizeof(vp8pd));
                    janus_vp8_simulcast_descriptor_update(payload, plen, &session->simulcast_context, switched);
                }
                /* Send the packet */
                if(gateway != NULL)
                    gateway->relay_rtp(session->handle, packet->is_video, (char *)packet->data, packet->length);
                /* Restore the timestamp and sequence number to what the publisher set them to */
                packet->data->timestamp = htonl(packet->timestamp);
                packet->data->seq_number = htons(packet->seq_number);
                if(packet->codec == JANUS_STREAMING_VP8) {
                    /* Restore the original payload descriptor as well, as it will be needed by the next viewer */
                    memcpy(payload, vp8pd, sizeof(vp8pd));
                }
            } else {
                /* Fix sequence number and timestamp (switching may be involved) */
                janus_rtp_header_update(packet->data, &session->context, TRUE, 4500);
                if(gateway != NULL)
                    gateway->relay_rtp(session->handle, packet->is_video, (char *)packet->data, packet->length);
                /* Restore the timestamp and sequence number to what the publisher set them to */
                packet->data->timestamp = htonl(packet->timestamp);
                packet->data->seq_number = htons(packet->seq_number);
            }
        } else {
            if(!session->audio)
                return;
            /* Fix sequence number and timestamp (switching may be involved) */
            janus_rtp_header_update(packet->data, &session->context, FALSE, 960);
            if(gateway != NULL)
                gateway->relay_rtp(session->handle, packet->is_video, (char *)packet->data, packet->length);
            /* Restore the timestamp and sequence number to what the publisher set them to */
            packet->data->timestamp = htonl(packet->timestamp);
            packet->data->seq_number = htons(packet->seq_number);
        }
    } else {
        /* We're broadcasting a data channel message */
        if(!session->data)
            return;
        char *text = (char *)packet->data;
        if(gateway != NULL && text != NULL)
            gateway->relay_data(session->handle, text, strlen(text));
    }

    return;
}
