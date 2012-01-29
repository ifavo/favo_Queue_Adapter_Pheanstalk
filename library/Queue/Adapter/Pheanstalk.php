<?php

/**
 * @see Zend_Queue_Adapter_AdapterAbstract
 */
require_once 'Zend/Queue/Adapter/AdapterAbstract.php';

/**
 * Class for using Beanstalkd as queuing system
 *
 * @category   favo
 * @package    favo_Queue
 * @subpackage Adapter
 * @copyright  Copyright (c) 2012 Mario Micklisch
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 */

require_once('ext/pheanstalk/pheanstalk_init.php');

class favo_Queue_Adapter_Pheanstalk extends Zend_Queue_Adapter_AdapterAbstract
{
    const DEFAULT_HOST = '127.0.0.1';
    const DEFAULT_PORT = 11300;
    const EOL          = "\r\n";

    /**
     * @var Pheanstalk
     */
    protected $_pheanstalk = null;

    /**
     * Constructor
     *
     * @param  array|Zend_Config $options
     * @param  null|Zend_Queue $queue
     * @return void
     */
    public function __construct($options = array(), Zend_Queue $queue = null)
    {
        parent::__construct($options, $queue);
        $options = &$this->_options['driverOptions'];

        if (!array_key_exists('host', $options)) {
            $options['host'] = self::DEFAULT_HOST;
        }
        if (!array_key_exists('port', $options)) {
            $options['port'] = self::DEFAULT_PORT;
        }

        $this->_pheanstalk = new Pheanstalk($options['host'], $options['port']);
    }

    /**
     * Destructor
     *
     * @return void
     */
    public function __destruct()
    {
        unset($this->_pheanstalk);
    }

    /********************************************************************
     * Queue management functions
     *********************************************************************/

    /**
     * Does a queue already exist?
     *
     * Throws an exception if the adapter cannot determine if a queue exists.
     * use isSupported('isExists') to determine if an adapter can test for
     * queue existance.
     *
     * @param  string $name
     * @return boolean
     * @throws Zend_Queue_Exception
     */
    public function isExists($name)
    {
        if (empty($this->_queues)) {
            $this->getQueues();
        }
        return in_array($name, $this->_queues);
    }

    /**
     * Create a new queue
     *
     * @param  string  $name    queue name
     * @param  integer $timeout default visibility timeout
     * @return boolean
     * @throws Zend_Queue_Exception
     */
    public function create($name, $timeout=null)
    {
    	$this->_pheanstalk->useTube($name)->put(null);
        return true;
    }

    /**
     * Delete a queue and all of it's messages
     *
     * @param  string  $name queue name
     * @return boolean
     * @throws Zend_Queue_Exception
     */
    public function delete($name)
    {
        return false;
    }

    /**
     * Get an array of all available queues
     *
     * @return array
     * @throws Zend_Queue_Exception
     */
    public function getQueues()
    {
        return $this->_queues = $this->_pheanstalk->listTubes();
    }

    /**
     * Return the approximate number of messages in the queue
     *
     * @param  Zend_Queue $queue
     * @return integer
     * @throws Zend_Queue_Exception (not supported)
     */
    public function count(Zend_Queue $queue=null)
    {
        if ($queue === null) {
            $queue = $this->_queue;
        }

        $stats = $this->_pheanstalk->statsTube($queue->getName());
        return $stats['current-jobs-ready'];
    }

    /********************************************************************
     * Messsage management functions
     *********************************************************************/

    /**
     * Send a message to the queue
     *
     * @param  string     $message Message to send to the active queue
     * @param  Zend_Queue $queue
     * @return Zend_Queue_Message
     * @throws Zend_Queue_Exception
     */
    public function send($message, Zend_Queue $queue=null)
    {
        if ($queue === null) {
            $queue = $this->_queue;
        }

        if (!$this->isExists($queue->getName())) {
            require_once 'Zend/Queue/Exception.php';
            throw new Zend_Queue_Exception('Queue does not exist:' . $queue->getName());
        }

        $message = (string) $message;
        $data    = array(
            'message_id' => md5(uniqid(rand(), true)),
            'handle'     => null,
            'body'       => $message,
            'md5'        => md5($message),
        );

        $result = $this->_pheanstalk->putInTube($queue->getName(), $message);
        if ($result === false) {
            require_once 'Zend/Queue/Exception.php';
            throw new Zend_Queue_Exception('failed to insert message into queue:' . $queue->getName());
        }

        $options = array(
            'queue' => $queue,
            'data'  => $data,
        );

        $classname = $queue->getMessageClass();
        if (!class_exists($classname)) {
            require_once 'Zend/Loader.php';
            Zend_Loader::loadClass($classname);
        }
        return new $classname($options);
    }

    /**
     * Get messages in the queue
     *
     * @param  integer    $maxMessages  Maximum number of messages to return
     * @param  integer    $timeout      Visibility timeout for these messages
     * @param  Zend_Queue $queue
     * @return Zend_Queue_Message_Iterator
     * @throws Zend_Queue_Exception
     */
    public function receive($maxMessages=null, $timeout=null, Zend_Queue $queue=null)
    {
        if ($maxMessages === null) {
            $maxMessages = 1;
        }

        if ($timeout === null) {
            $timeout = self::RECEIVE_TIMEOUT_DEFAULT;
        }
        if ($queue === null) {
            $queue = $this->_queue;
        }

        $msgs = array();
        if ($maxMessages > 0 ) {
            for ($i = 0; $i < $maxMessages; $i++) {
            	$job = $this->_pheanstalk->watch($queue->getName())->reserve();
                $data = array(
                    'handle' => $job,
                    'body'   => $job->getData(),
                );
                $msgs[] = $data;
            }
        }

        $options = array(
            'queue'        => $queue,
            'data'         => $msgs,
            'messageClass' => $queue->getMessageClass(),
        );

        $classname = $queue->getMessageSetClass();
        if (!class_exists($classname)) {
            require_once 'Zend/Loader.php';
            Zend_Loader::loadClass($classname);
        }
        return new $classname($options);
    }

    /**
     * Delete a message from the queue
     *
     * Returns true if the message is deleted, false if the deletion is
     * unsuccessful.
     *
     * @param  Zend_Queue_Message $message
     * @return boolean
     * @throws Zend_Queue_Exception (unsupported)
     */
    public function deleteMessage(Zend_Queue_Message $message)
    {
    	return $this->_pheanstalk->delete($message->handle);
    }

    /********************************************************************
     * Supporting functions
     *********************************************************************/

    /**
     * Return a list of queue capabilities functions
     *
     * $array['function name'] = true or false
     * true is supported, false is not supported.
     *
     * @param  string $name
     * @return array
     */
    public function getCapabilities()
    {
        return array(
            'create'        => true,
            'delete'        => false,
            'send'          => true,
            'receive'       => true,
            'deleteMessage' => true,
            'getQueues'     => true,
            'count'         => true,
            'isExists'      => true
        );
    }

}
