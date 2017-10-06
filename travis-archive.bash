BIN_DIR="${HOME}/.cache/bin"
MINIO_CLIENT="${BIN_DIR}/mc"

START_TIME=`date +%s`

if [[ -n "${OS}" && -n "${DIST}" ]]; then
    echo "Uploader: build data isn't found, skipping"
    exit 0
fi

if [[ ! -x "${MINIO_CLIENT}" ]]; then
    echo "Uploader: couldn't find minio client - downloading"
    mkdir -p "${BIN_DIR}"
    rm -rf "${MINIO_CLIENT}"
    wget https://dl.minio.io/server/minio/release/linux-amd64/minio -O ${MINIO_CLIENT}
    chmod +x ${MINIO_CLIENT}
fi

${MINIO_CLIENT} config host add tarantool-s3 http://s3.tarantool.org ${S3_ACCESS_KEY} ${S3_SECRET_KEY}

DEB_DST="tarantool-s3/archive/${OS}/${DIST}/os/x86_64/"
RPM_DST="tarantool-s3/archive/${OS}/pool/main/${PROJECT}"

COUNT=0

for fullname in "$@"; do
    PACKAGE_NAME=`basename "$fullname"`
    PACKAGE_EXTENSION="${filename##*.}"
    echo -n "Uploader: uploading file $fullname "

    case "${OS}" in
        "el") ;;
        "fedora")
            echo "to ${DEB_DST}"
            ${MINIO_CLIENT} mb ${DEB_DST}
            ${MINIO_CLIENT} cp $fullname ${DEB_DST}
            ;&
        "ubuntu") ;;
        "debian")
            echo "to ${RPM_DST}"
            ${MINIO_CLIENT} mb ${RPM_DST}
            ${MINIO_CLIENT} cp $fullname ${RPM_DST}
            ;&
        *)
            echo
            echo "Uploader: unknown OS '${OS}'"
            exit 1
            ;;
    esac
    COUNT=$((COUNT+1))
done

RUN_TIME=$((START_TIME-`date +%s`))

echo "Finished uploading $COUNT in ${RUN_TIME} sec"
